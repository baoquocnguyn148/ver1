"""
Customer churn modeling pipeline.

Runs an end-to-end, reproducible workflow from data/ver1.xlsx:
1. Load and validate the Data Model sheet.
2. Build customer-level features at monthly snapshot dates.
3. Benchmark Logistic Regression, Random Forest, and XGBoost on a strict time split.
4. Select the best model from the November backtest.
5. Retrain on available labeled snapshots and score December churn risk.
6. Export Excel outputs for operation and audit.
"""

from __future__ import annotations

import json
import math
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

LOCAL_SITE_PACKAGES = Path(__file__).resolve().parent / ".venv" / "Lib" / "site-packages"
if LOCAL_SITE_PACKAGES.exists():
    sys.path.insert(0, str(LOCAL_SITE_PACKAGES))

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    fbeta_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

try:
    from xgboost import XGBClassifier

    HAS_XGBOOST = True
except Exception:
    XGBClassifier = None
    HAS_XGBOOST = False


warnings.filterwarnings("ignore", category=UserWarning)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "outputs"
INPUT_FILE = DATA_DIR / "ver1.xlsx"
DATA_SHEET = "Data Model"
LOOKBACK_DAYS = 90

SNAPSHOT_SEP = pd.Timestamp("2023-09-30")
SNAPSHOT_OCT = pd.Timestamp("2023-10-31")
SNAPSHOT_NOV = pd.Timestamp("2023-11-30")

FEATURE_COLS = [
    "Segment",
    "Recency",
    "Frequency",
    "AOV",
    "PromoRate",
    "Margin",
    "Trend",
    "DaysSinceFirst",
    "ActiveMonths",
]
NUMERIC_FEATURES = [c for c in FEATURE_COLS if c != "Segment"]
CATEGORICAL_FEATURES = ["Segment"]

OUTPUT_SCORING = OUTPUT_DIR / "churn_list.xlsx"
OUTPUT_COMPARISON = OUTPUT_DIR / "churn_model_comparison.xlsx"
OUTPUT_BACKTEST = OUTPUT_DIR / "churn_backtest_nov.xlsx"
OUTPUT_DEEPDIVE = OUTPUT_DIR / "churn_deepdive_data.xlsx"
OUTPUT_RATE_REPORT = OUTPUT_DIR / "churn_rate_report.xlsx"
OUTPUT_METADATA = OUTPUT_DIR / "model_selection_summary.json"


@dataclass(frozen=True)
class ModelSpec:
    name: str
    factory: Callable[[], Pipeline]


def load_and_clean_data(path: Path = INPUT_FILE, sheet_name: str = DATA_SHEET) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input workbook not found: {path}")

    df = pd.read_excel(path, sheet_name=sheet_name)
    required_cols = {
        "Ngày Xuất",
        "Khách hàng",
        "Segment",
        "Doanh Thu",
        "Lợi Nhuận",
        "% SL Khuyến mãi",
    }
    missing = sorted(required_cols - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    df = df.copy()
    df["Ngày Xuất"] = pd.to_datetime(df["Ngày Xuất"], errors="coerce", dayfirst=True)
    df["Khách hàng"] = df["Khách hàng"].astype(str).str.strip()
    df["Segment"] = df["Segment"].astype(str).str.strip()
    for col in ["Doanh Thu", "Lợi Nhuận", "% SL Khuyến mãi"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["Ngày Xuất", "Khách hàng"])
    return df.sort_values("Ngày Xuất").reset_index(drop=True)


def build_features(df: pd.DataFrame, as_of_date: pd.Timestamp, lookback_days: int = LOOKBACK_DAYS) -> pd.DataFrame:
    start = as_of_date - pd.Timedelta(days=lookback_days)
    hist = df[(df["Ngày Xuất"] > start) & (df["Ngày Xuất"] <= as_of_date)].copy()
    if hist.empty:
        return pd.DataFrame(columns=["Khách hàng", *FEATURE_COLS])

    agg = hist.groupby("Khách hàng").agg(
        first_purchase=("Ngày Xuất", "min"),
        last_purchase=("Ngày Xuất", "max"),
        Frequency=("Khách hàng", "size"),
        Revenue=("Doanh Thu", "sum"),
        Profit=("Lợi Nhuận", "sum"),
        PromoRate=("% SL Khuyến mãi", "mean"),
    )
    agg["Recency"] = (as_of_date - agg["last_purchase"]).dt.days
    agg["DaysSinceFirst"] = (as_of_date - agg["first_purchase"]).dt.days
    agg["AOV"] = np.where(agg["Frequency"] > 0, agg["Revenue"] / agg["Frequency"], 0)
    agg["Margin"] = np.where(agg["Revenue"] != 0, agg["Profit"] / agg["Revenue"], 0)

    recent = hist[hist["Ngày Xuất"] > as_of_date - pd.Timedelta(days=30)]
    older = hist[
        (hist["Ngày Xuất"] <= as_of_date - pd.Timedelta(days=30))
        & (hist["Ngày Xuất"] > as_of_date - pd.Timedelta(days=60))
    ]
    recent_rev = recent.groupby("Khách hàng")["Doanh Thu"].sum()
    older_rev = older.groupby("Khách hàng")["Doanh Thu"].sum()
    agg["Trend"] = (recent_rev - older_rev).reindex(agg.index).fillna(0)

    hist["Month"] = hist["Ngày Xuất"].dt.to_period("M")
    agg["ActiveMonths"] = hist.groupby("Khách hàng")["Month"].nunique().reindex(agg.index).fillna(0)

    seg_lookup = (
        df[df["Ngày Xuất"] <= as_of_date]
        .sort_values("Ngày Xuất")
        .groupby("Khách hàng")["Segment"]
        .last()
    )
    agg["Segment"] = seg_lookup.reindex(agg.index).fillna("Unknown")

    out = agg.reset_index()[["Khách hàng", *FEATURE_COLS]]
    out[NUMERIC_FEATURES] = out[NUMERIC_FEATURES].replace([np.inf, -np.inf], 0).fillna(0)
    return out


def label_churn_window(
    df: pd.DataFrame,
    customers: pd.Series,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.Series:
    future_buyers = set(
        df[(df["Ngày Xuất"] >= window_start) & (df["Ngày Xuất"] <= window_end)]["Khách hàng"].unique()
    )
    return customers.apply(lambda customer: 0 if customer in future_buyers else 1).astype(int)


def preprocess(scale_numeric: bool) -> ColumnTransformer:
    numeric_transformer = StandardScaler() if scale_numeric else "passthrough"
    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ]
    )


def get_model_specs() -> list[ModelSpec]:
    specs = [
        ModelSpec(
            "logistic_regression",
            lambda: Pipeline(
                [
                    ("prep", preprocess(scale_numeric=True)),
                    (
                        "model",
                        LogisticRegression(
                            C=0.5,
                            class_weight={0: 1, 1: 2},
                            max_iter=3000,
                            random_state=42,
                        ),
                    ),
                ]
            ),
        ),
        ModelSpec(
            "random_forest_calibrated",
            lambda: CalibratedClassifierCV(
                Pipeline(
                    [
                        ("prep", preprocess(scale_numeric=False)),
                        (
                            "model",
                            RandomForestClassifier(
                                n_estimators=300,
                                max_depth=4,
                                min_samples_leaf=5,
                                class_weight={0: 1, 1: 2},
                                random_state=42,
                            ),
                        ),
                    ]
                ),
                method="sigmoid",
                cv=3,
            ),
        ),
    ]

    if HAS_XGBOOST:
        specs.append(
            ModelSpec(
                "xgboost",
                lambda: Pipeline(
                    [
                        ("prep", preprocess(scale_numeric=False)),
                        (
                            "model",
                            XGBClassifier(
                                n_estimators=120,
                                max_depth=2,
                                learning_rate=0.05,
                                subsample=0.9,
                                colsample_bytree=0.9,
                                min_child_weight=2,
                                reg_lambda=3,
                                reg_alpha=0.2,
                                objective="binary:logistic",
                                eval_metric="logloss",
                                random_state=42,
                            ),
                        ),
                    ]
                ),
            )
        )
    return specs


def choose_threshold(prob: np.ndarray, y_true: np.ndarray, max_urgent: int = 55, min_recall: float = 0.72) -> tuple[float, dict]:
    candidates = np.unique(np.r_[np.linspace(0.05, 0.95, 181), prob])
    rows = []
    for threshold in candidates:
        pred = (prob >= threshold).astype(int)
        rows.append(
            {
                "threshold": float(threshold),
                "precision": float(precision_score(y_true, pred, zero_division=0)),
                "recall": float(recall_score(y_true, pred, zero_division=0)),
                "f2": float(fbeta_score(y_true, pred, beta=2, zero_division=0)),
                "urgent_count": int(pred.sum()),
            }
        )
    grid = pd.DataFrame(rows)
    feasible = grid[(grid["recall"] >= min_recall) & (grid["urgent_count"] <= max_urgent)]
    if feasible.empty:
        best = grid.sort_values(["f2", "recall", "precision"], ascending=False).iloc[0]
    else:
        best = feasible.sort_values(["f2", "precision", "threshold"], ascending=False).iloc[0]
    return float(best["threshold"]), best.to_dict()


def risk_level(prob: float, urgent_threshold: float) -> str:
    high_threshold = max(0.35, urgent_threshold - 0.20)
    medium_threshold = max(0.20, high_threshold - 0.20)
    if prob >= urgent_threshold:
        return "Khẩn cấp"
    if prob >= high_threshold:
        return "Cao"
    if prob >= medium_threshold:
        return "Trung bình"
    return "Thấp"


def action_rule(row: pd.Series, urgent_threshold: float) -> str:
    if row["Mức độ"] == "Khẩn cấp" and row.get("Segment") == "Khách hàng VIP":
        return "Gặp mặt trực tiếp"
    if row["Mức độ"] == "Khẩn cấp" or row["Recency"] > 60:
        return "Gọi ngay"
    if row["Mức độ"] == "Cao" or row["Recency"] > 30:
        return "Zalo offer"
    return "Theo dõi định kỳ"


def safe_write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> Path:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sheet_name, frame in sheets.items():
                frame.to_excel(writer, sheet_name=sheet_name, index=False)
        return path
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_new{path.suffix}")
        with pd.ExcelWriter(fallback, engine="openpyxl") as writer:
            for sheet_name, frame in sheets.items():
                frame.to_excel(writer, sheet_name=sheet_name, index=False)
        return fallback


def benchmark_models(df: pd.DataFrame) -> tuple[str, float, pd.DataFrame, pd.DataFrame, dict]:
    f_train = build_features(df, SNAPSHOT_SEP)
    y_train = label_churn_window(df, f_train["Khách hàng"], pd.Timestamp("2023-10-01"), pd.Timestamp("2023-10-31"))
    f_test = build_features(df, SNAPSHOT_OCT)
    y_test = label_churn_window(df, f_test["Khách hàng"], pd.Timestamp("2023-11-01"), pd.Timestamp("2023-11-30"))

    rows = []
    predictions = f_test[["Khách hàng", "Segment", *NUMERIC_FEATURES]].copy()
    model_objects = {}

    for spec in get_model_specs():
        model = spec.factory()
        model.fit(f_train[FEATURE_COLS], y_train.values)
        prob = model.predict_proba(f_test[FEATURE_COLS])[:, 1]
        threshold, threshold_info = choose_threshold(prob, y_test.values)
        pred = (prob >= threshold).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_test, pred, labels=[0, 1]).ravel()

        rows.append(
            {
                "model": spec.name,
                "train_customers": int(len(f_train)),
                "test_customers": int(len(f_test)),
                "test_churn_rate_actual": float(y_test.mean()),
                "test_churn_rate_predicted_mean_prob": float(prob.mean()),
                "roc_auc": float(roc_auc_score(y_test, prob)),
                "average_precision": float(average_precision_score(y_test, prob)),
                "threshold": float(threshold),
                "precision": float(precision_score(y_test, pred, zero_division=0)),
                "recall": float(recall_score(y_test, pred, zero_division=0)),
                "f2": float(fbeta_score(y_test, pred, beta=2, zero_division=0)),
                "tn": int(tn),
                "fp": int(fp),
                "fn": int(fn),
                "tp": int(tp),
                "threshold_reason": json.dumps(threshold_info, ensure_ascii=False),
            }
        )
        predictions[f"{spec.name}_prob"] = prob
        predictions[f"{spec.name}_pred"] = pred
        model_objects[spec.name] = model

    comparison = pd.DataFrame(rows).sort_values(
        ["roc_auc", "average_precision", "f2", "precision"],
        ascending=False,
    )
    best_model = str(comparison.iloc[0]["model"])
    best_threshold = float(comparison.iloc[0]["threshold"])

    backtest = predictions.copy()
    backtest["ActualChurn"] = y_test.values
    backtest["BestModel"] = best_model
    backtest["ChurnProb"] = backtest[f"{best_model}_prob"]
    backtest["PredictedChurn"] = (backtest["ChurnProb"] >= best_threshold).astype(int)

    return best_model, best_threshold, comparison, backtest, model_objects


def forecast_december(df: pd.DataFrame, best_model_name: str, threshold: float) -> pd.DataFrame:
    f_sep = build_features(df, SNAPSHOT_SEP)
    y_sep = label_churn_window(df, f_sep["Khách hàng"], pd.Timestamp("2023-10-01"), pd.Timestamp("2023-10-31"))
    f_oct = build_features(df, SNAPSHOT_OCT)
    y_oct = label_churn_window(df, f_oct["Khách hàng"], pd.Timestamp("2023-11-01"), pd.Timestamp("2023-11-30"))

    train = pd.concat([f_sep, f_oct], ignore_index=True)
    y_train = pd.concat([y_sep, y_oct], ignore_index=True).values
    score = build_features(df, SNAPSHOT_NOV)

    spec_by_name = {spec.name: spec for spec in get_model_specs()}
    model = spec_by_name[best_model_name].factory()
    model.fit(train[FEATURE_COLS], y_train)

    out = score[["Khách hàng", "Segment", *NUMERIC_FEATURES]].copy()
    out["ChurnProb"] = model.predict_proba(score[FEATURE_COLS])[:, 1]
    out["RiskScore"] = (out["ChurnProb"] * 10).clip(1, 10).round(1)
    out["Mức độ"] = out["ChurnProb"].apply(lambda p: risk_level(float(p), threshold))
    out["Hành động"] = out.apply(lambda row: action_rule(row, threshold), axis=1)

    return out.sort_values(["ChurnProb", "Recency"], ascending=False).reset_index(drop=True)


def build_rate_report(scored: pd.DataFrame) -> pd.DataFrame:
    return (
        scored.groupby("Segment", dropna=False)
        .agg(
            customers=("Khách hàng", "count"),
            mean_churn_prob=("ChurnProb", "mean"),
            urgent_customers=("Mức độ", lambda s: int((s == "Khẩn cấp").sum())),
            high_or_urgent=("Mức độ", lambda s: int(s.isin(["Cao", "Khẩn cấp"]).sum())),
            avg_recency=("Recency", "mean"),
            avg_aov=("AOV", "mean"),
            avg_margin=("Margin", "mean"),
        )
        .reset_index()
        .sort_values("mean_churn_prob", ascending=False)
    )


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    df = load_and_clean_data()
    best_model, threshold, comparison, backtest, _ = benchmark_models(df)
    scored = forecast_december(df, best_model, threshold)
    rate_report = build_rate_report(scored)

    best_row = comparison.iloc[0].to_dict()
    metadata = {
        "input_file": str(INPUT_FILE.name),
        "data_sheet": DATA_SHEET,
        "lookback_days": LOOKBACK_DAYS,
        "train_snapshot": str(SNAPSHOT_SEP.date()),
        "test_snapshot": str(SNAPSHOT_OCT.date()),
        "forecast_snapshot": str(SNAPSHOT_NOV.date()),
        "selected_model": best_model,
        "selected_threshold": threshold,
        "selection_metric_order": ["roc_auc", "average_precision", "f2", "precision"],
        "best_backtest": {
            key: (float(value) if isinstance(value, (np.floating, float)) and math.isfinite(value) else value)
            for key, value in best_row.items()
        },
        "warnings": [
            "Only one strict monthly holdout is available because the source data covers 2023-09 through 2023-11.",
            "December churn labels are not available in data/ver1.xlsx, so December output is a forecast only.",
        ],
    }

    safe_write_excel(OUTPUT_COMPARISON, {"model_comparison": comparison})
    safe_write_excel(OUTPUT_BACKTEST, {"nov_backtest": backtest})
    safe_write_excel(
        OUTPUT_DEEPDIVE,
        {
            "nov_summary": comparison[
                ["model", "tn", "fp", "fn", "tp", "threshold", "precision", "recall", "f2", "roc_auc"]
            ]
        },
    )
    safe_write_excel(OUTPUT_RATE_REPORT, {"segment_rate_report": rate_report})
    safe_write_excel(
        OUTPUT_SCORING,
        {
            "churn_scoring": scored[
                [
                    "Khách hàng",
                    "Segment",
                    "Recency",
                    "Frequency",
                    "AOV",
                    "Margin",
                    "Trend",
                    "ActiveMonths",
                    "RiskScore",
                    "ChurnProb",
                    "Mức độ",
                    "Hành động",
                ]
            ]
        },
    )
    OUTPUT_METADATA.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Selected model:", best_model)
    print("Selected threshold:", f"{threshold:.4f}")
    print(comparison.to_string(index=False))
    print("Saved:", OUTPUT_SCORING.name, OUTPUT_COMPARISON.name, OUTPUT_BACKTEST.name, OUTPUT_RATE_REPORT.name)


if __name__ == "__main__":
    main()
