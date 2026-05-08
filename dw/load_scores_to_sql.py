"""
Load outputs/churn_list.xlsx into SQL Server staging table stg.ChurnScoreRaw.

Required environment variable:
    CHURN_DW_CONN_STR

Optional environment variables:
    CHURN_DW_SCORE_FILE
    CHURN_DW_SNAPSHOT_DATE
    CHURN_DW_MODEL_NAME
    CHURN_DW_MODEL_VERSION
    CHURN_DW_APPLY_SCORE
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL_SITE_PACKAGES = ROOT / ".venv" / "Lib" / "site-packages"
if LOCAL_SITE_PACKAGES.exists():
    sys.path.insert(0, str(LOCAL_SITE_PACKAGES))

import pandas as pd

try:
    import pyodbc
except ImportError as exc:
    raise SystemExit("pyodbc is required. Install dependencies with: pip install -r requirements.txt") from exc


SCORE_FILE = Path(os.environ.get("CHURN_DW_SCORE_FILE", ROOT / "outputs" / "churn_list.xlsx"))
SNAPSHOT_DATE = os.environ.get("CHURN_DW_SNAPSHOT_DATE", "2023-11-30")
MODEL_NAME = os.environ.get("CHURN_DW_MODEL_NAME", "random_forest_calibrated")
MODEL_VERSION = os.environ.get("CHURN_DW_MODEL_VERSION", "rf_calibrated_2026_05_08")
CONN_STR = os.environ.get("CHURN_DW_CONN_STR")
APPLY_SCORE = os.environ.get("CHURN_DW_APPLY_SCORE", "1").strip().lower() in {"1", "true", "yes"}


def main() -> None:
    if not CONN_STR:
        raise SystemExit("Set CHURN_DW_CONN_STR before running this loader.")
    if not SCORE_FILE.exists():
        raise FileNotFoundError(f"Score workbook not found: {SCORE_FILE}")

    df = pd.read_excel(SCORE_FILE, sheet_name="churn_scoring")
    required = {"Khách hàng", "ChurnProb"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required score columns: {missing}")

    if "RiskScore" not in df.columns:
        df["RiskScore"] = df["ChurnProb"] * 10

    rows = [
        (
            None,
            SNAPSHOT_DATE,
            str(row["Khách hàng"]).strip(),
            MODEL_NAME,
            MODEL_VERSION,
            float(row["ChurnProb"]),
            float(row["RiskScore"]) if pd.notna(row["RiskScore"]) else None,
            SCORE_FILE.name,
        )
        for _, row in df.iterrows()
    ]

    with pyodbc.connect(CONN_STR, autocommit=False) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            DECLARE @BatchId UNIQUEIDENTIFIER;
            EXEC audit.sp_StartEtlBatch
                @PipelineName = ?,
                @SourceFileName = ?,
                @LoadBatchId = @BatchId OUTPUT;
            SELECT @BatchId;
            """,
            "load_scores_to_sql.py",
            str(SCORE_FILE),
        )
        load_batch_id = cursor.fetchone()[0]

        rows = [(load_batch_id, *row[1:]) for row in rows]
        cursor.fast_executemany = True
        cursor.executemany(
            """
            INSERT INTO stg.ChurnScoreRaw
                (LoadBatchId, SnapshotDate, CustomerName, ModelName, ModelVersion, ChurnProb, RiskScore, SourceFileName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        if APPLY_SCORE:
            cursor.execute(
                """
                EXEC dw.sp_Score_LoadFromStaging
                    @SnapshotDate = ?,
                    @LoadBatchId = ?,
                    @ModelName = ?,
                    @ModelVersion = ?
                """,
                SNAPSHOT_DATE,
                load_batch_id,
                MODEL_NAME,
                MODEL_VERSION,
            )

        cursor.execute(
            "EXEC audit.sp_FinishEtlBatch @LoadBatchId = ?, @Status = ?, @RowsLoaded = ?",
            load_batch_id,
            "SUCCESS",
            len(rows),
        )
        conn.commit()

    print(f"Loaded {len(rows)} scores into stg.ChurnScoreRaw. LoadBatchId={load_batch_id}")


if __name__ == "__main__":
    main()
