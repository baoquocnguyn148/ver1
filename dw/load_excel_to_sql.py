"""
Load data/ver1.xlsx into SQL Server staging table stg.SalesRaw.

Required environment variable:
    CHURN_DW_CONN_STR

Optional environment variables:
    CHURN_DW_SOURCE_FILE
    CHURN_DW_SOURCE_SHEET
    CHURN_DW_RUN_ETL
"""

from __future__ import annotations

import hashlib
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


SOURCE_FILE = Path(os.environ.get("CHURN_DW_SOURCE_FILE", ROOT / "data" / "ver1.xlsx"))
SOURCE_SHEET = os.environ.get("CHURN_DW_SOURCE_SHEET", "Data Model")
CONN_STR = os.environ.get("CHURN_DW_CONN_STR")
RUN_ETL = os.environ.get("CHURN_DW_RUN_ETL", "0").strip().lower() in {"1", "true", "yes"}


COLUMN_MAP = {
    "STT": "SourceRowNumber",
    "Mã": "ProductCode",
    "Hàng": "ProductName",
    "ĐVT": "UnitName",
    "Tiền Nhập": "ImportAmount",
    "SL Xuất KM": "PromoQuantity",
    "SL Xuất": "SalesQuantity",
    "Tiền Xuất": "ExportAmount",
    "Doanh Thu": "Revenue",
    "Ngày Xuất": "SaleDate",
    "Khách hàng": "CustomerName",
    "Segment": "SegmentName",
    "Lợi Nhuận": "Profit",
    "Tỷ lệ lợi nhuận": "MarginPct",
    "Tổng số lượng bán": "TotalQuantitySold",
    "% SL Khuyến mãi": "PromoPct",
    "Year": "YearNumber",
    "Month": "MonthNumber",
    "Ngành Hàng": "CategoryName",
}

TARGET_COLUMNS = [
    "LoadBatchId",
    "SourceFileName",
    "SourceRowNumber",
    "ProductCode",
    "ProductName",
    "UnitName",
    "ImportAmount",
    "PromoQuantity",
    "SalesQuantity",
    "ExportAmount",
    "Revenue",
    "SaleDate",
    "CustomerName",
    "SegmentName",
    "Profit",
    "MarginPct",
    "TotalQuantitySold",
    "PromoPct",
    "YearNumber",
    "MonthNumber",
    "CategoryName",
    "RowHash",
]


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def build_row_hash(row: pd.Series) -> bytes:
    stable = "|".join("" if pd.isna(v) else str(v) for v in row.tolist())
    return hashlib.sha256(stable.encode("utf-8")).digest()


def load_source() -> pd.DataFrame:
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source workbook not found: {SOURCE_FILE}")

    df = pd.read_excel(SOURCE_FILE, sheet_name=SOURCE_SHEET)
    missing = sorted(set(COLUMN_MAP) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required source columns: {missing}")

    df = df.rename(columns=COLUMN_MAP)[list(COLUMN_MAP.values())].copy()
    df["SaleDate"] = pd.to_datetime(df["SaleDate"], errors="coerce", dayfirst=True)

    text_cols = ["ProductCode", "ProductName", "UnitName", "CustomerName", "SegmentName", "CategoryName"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    numeric_cols = [
        "ImportAmount",
        "PromoQuantity",
        "SalesQuantity",
        "ExportAmount",
        "Revenue",
        "Profit",
        "MarginPct",
        "TotalQuantitySold",
        "PromoPct",
        "YearNumber",
        "MonthNumber",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["SourceRowNumber", "ProductCode", "SaleDate", "CustomerName"])
    df["SourceRowNumber"] = df["SourceRowNumber"].astype(int)
    df["RowHash"] = df[list(COLUMN_MAP.values())].apply(build_row_hash, axis=1)
    return df


def main() -> None:
    if not CONN_STR:
        raise SystemExit("Set CHURN_DW_CONN_STR before running this loader.")

    df = load_source()

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
            "load_excel_to_sql.py",
            str(SOURCE_FILE),
        )
        load_batch_id = cursor.fetchone()[0]

        insert_sql = f"""
            INSERT INTO stg.SalesRaw ({", ".join(TARGET_COLUMNS)})
            VALUES ({", ".join(["?"] * len(TARGET_COLUMNS))})
        """
        rows = []
        for _, row in df.iterrows():
            payload = {
                "LoadBatchId": load_batch_id,
                "SourceFileName": str(SOURCE_FILE.name),
                **{col: clean_value(row[col]) for col in COLUMN_MAP.values()},
                "RowHash": row["RowHash"],
            }
            rows.append(tuple(payload[col] for col in TARGET_COLUMNS))

        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)

        if RUN_ETL:
            cursor.execute("EXEC dw.sp_ETL_RunAll @LoadBatchId = ?", load_batch_id)

        cursor.execute(
            "EXEC audit.sp_FinishEtlBatch @LoadBatchId = ?, @Status = ?, @RowsLoaded = ?",
            load_batch_id,
            "SUCCESS",
            len(rows),
        )
        conn.commit()

    print(f"Loaded {len(rows)} rows into stg.SalesRaw. LoadBatchId={load_batch_id}")


if __name__ == "__main__":
    main()
