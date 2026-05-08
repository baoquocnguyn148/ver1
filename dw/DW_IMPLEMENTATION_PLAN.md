# Data Warehouse Implementation Plan

Plan này thay thế bản DW cũ và bám sát project hiện tại:

- Source of truth dữ liệu giao dịch: `data/ver1.xlsx`, sheet `Data Model`.
- Pipeline ML hiện hành: `churn_pipeline_main.py`.
- Model vận hành đã benchmark: `random_forest_calibrated`.
- Output vận hành: `outputs/churn_list.xlsx`.
- DBMS mục tiêu: Microsoft SQL Server.

## 1. Mục tiêu DW

DW không thay thế hoàn toàn Python ML. DW chịu trách nhiệm làm source of truth cho dữ liệu bán hàng, feature snapshot, score history và reporting. Python giữ vai trò train/predict model.

Luồng chuẩn:

```text
data/ver1.xlsx
  -> stg.SalesRaw
  -> dw.DimDate, dw.DimCustomer, dw.DimProduct, dw.FactSales
  -> dw.CustomerSnapshot
  -> Python Random Forest calibrated
  -> stg.ChurnScoreRaw
  -> dw.FactChurnScore
  -> report stored procedures / Power BI
```

## 2. Nguyên tắc thiết kế

1. Không dùng tên cột tiếng Việt trong bảng DW lõi. Tên cột nội bộ dùng English/ASCII để dễ tích hợp.
2. Staging giữ mapping đầy đủ từ Excel, gồm `SourceRowNumber` từ `STT` để chống trùng.
3. Không tự phân loại `Ngành Hàng` bằng CASE trong SQL. Source đã có `Ngành Hàng`, DW dùng trực tiếp để tránh sai category.
4. Feature engineering trong SQL phải khớp `churn_pipeline_main.py`:
   - lookback 90 ngày
   - `SaleDate > SnapshotDate - 90` và `SaleDate <= SnapshotDate`
   - chỉ customer có giao dịch trong lookback mới có snapshot
   - `Trend = revenue last 30 days - revenue previous 30 days`
5. Historical segment phải lấy theo giao dịch gần nhất trước hoặc bằng snapshot, không dùng segment tương lai.
6. Score/risk rule phải khớp output hiện tại:
   - `UrgentThreshold = 0.5380230930245646`
   - `HighThreshold = 0.35`
   - `MediumThreshold = 0.20`
   - VIP + Khẩn cấp -> `Gặp mặt trực tiếp`
   - Khẩn cấp hoặc `Recency > 60` -> `Gọi ngay`
   - Cao hoặc `Recency > 30` -> `Zalo offer`
   - còn lại -> `Theo dõi định kỳ`

## 3. Schemas

| Schema | Vai trò |
|---|---|
| `stg` | Landing/staging từ Excel và Python scoring |
| `dw` | Dim/fact/snapshot/reporting tables |
| `cfg` | Model version, threshold, runtime parameters |
| `audit` | Batch metadata, load status |

## 4. Core tables

### `stg.SalesRaw`

Nhận dữ liệu từ `data/ver1.xlsx`. Tên cột đã chuẩn hóa sang English:

- `SourceRowNumber`, `ProductCode`, `ProductName`, `UnitName`
- `ImportAmount`, `PromoQuantity`, `SalesQuantity`, `ExportAmount`
- `Revenue`, `SaleDate`, `CustomerName`, `SegmentName`
- `Profit`, `MarginPct`, `TotalQuantitySold`, `PromoPct`
- `YearNumber`, `MonthNumber`, `CategoryName`
- `SourceFileName`, `LoadBatchId`, `RowHash`

### `dw.FactSales`

Fact giao dịch trung tâm, grain = một dòng giao dịch từ Excel.

Unique key: `(SourceSystem, SourceRowNumber)`.

Giữ thêm `SegmentAtSale` để build historical snapshot không bị leakage.

### `dw.CustomerSnapshot`

Feature table phục vụ model:

- `Segment`
- `Recency`
- `Frequency`
- `AOV`
- `PromoRate`
- `Margin`
- `Trend`
- `DaysSinceFirst`
- `ActiveMonths`
- `ChurnLabel`

Primary key: `(SnapshotDate, CustomerKey)`.

### `dw.FactChurnScore`

Lưu score theo snapshot, customer, model version:

- `ChurnProb`
- `RiskScore`
- `RiskLevel`
- `SalesAction`
- `ModelName`
- `ModelVersion`

## 5. Stored procedures

| Procedure | Mục đích |
|---|---|
| `cfg.sp_SetDefaultConfig` | Seed threshold và model version hiện hành |
| `audit.sp_StartEtlBatch` | Tạo batch load |
| `audit.sp_FinishEtlBatch` | Cập nhật trạng thái batch |
| `dw.sp_ETL_LoadDimDate` | Sinh date dimension |
| `dw.sp_ETL_LoadDimensions` | Upsert customer/product |
| `dw.sp_ETL_LoadFactSales` | Merge staging vào fact |
| `dw.sp_ETL_RunAll` | Chạy DimDate -> Dimensions -> Fact |
| `dw.sp_FE_BuildCustomerSnapshot` | Build feature snapshot 90 ngày |
| `dw.sp_FE_BuildChurnLabel` | Gắn nhãn churn cho backtest/retrain |
| `dw.sp_Score_LoadFromStaging` | Load score từ `stg.ChurnScoreRaw` vào fact score |
| `dw.sp_Score_UpsertChurnScore` | Upsert một score đơn lẻ |
| `dw.sp_Score_ApplyBusinessRules` | Gắn risk/action |
| `dw.sp_Report_*` | Report cho Power BI / sales |

## 6. Python integration

Thêm script:

- `dw/load_excel_to_sql.py`: đọc `data/ver1.xlsx` và insert vào `stg.SalesRaw`.
- `dw/load_scores_to_sql.py`: đọc `outputs/churn_list.xlsx` và insert vào `stg.ChurnScoreRaw`.

Kết nối dùng environment variables:

- `CHURN_DW_CONN_STR`: pyodbc connection string đầy đủ.
- `CHURN_DW_SOURCE_FILE`: override source workbook nếu cần.
- `CHURN_DW_SCORE_FILE`: override score workbook nếu cần.

## 7. Runbook

1. Tạo database `ChurnDW`.
2. Chạy `dw/DW_Master_Script_Full.sql`.
3. Load Excel:

```bash
python dw/load_excel_to_sql.py
```

4. Chạy ETL trong SQL:

```sql
EXEC dw.sp_ETL_RunAll @LoadBatchId = NULL;
EXEC dw.sp_FE_BuildCustomerSnapshot @SnapshotDate = '2023-09-30';
EXEC dw.sp_FE_BuildChurnLabel @SnapshotDate = '2023-09-30';
EXEC dw.sp_FE_BuildCustomerSnapshot @SnapshotDate = '2023-10-31';
EXEC dw.sp_FE_BuildChurnLabel @SnapshotDate = '2023-10-31';
EXEC dw.sp_FE_BuildCustomerSnapshot @SnapshotDate = '2023-11-30';
```

5. Python train/predict bằng `churn_pipeline_main.py`.
6. Load score:

```bash
python dw/load_scores_to_sql.py
```

7. Trong SQL:

```sql
EXEC dw.sp_Score_LoadFromStaging @SnapshotDate = '2023-11-30';
EXEC dw.sp_Report_ChurnList @SnapshotDate = '2023-11-30';
```

## 8. Production hardening

Khi có thêm dữ liệu tháng mới:

1. Giữ toàn bộ score history, không overwrite output cũ.
2. Thêm rolling backtest nhiều tháng vào `dw.CustomerSnapshot`.
3. Cập nhật `cfg.ModelVersion` khi model mới được chọn.
4. Đưa ingestion và scoring vào SQL Agent / orchestrator.
5. Thêm kiểm tra reconciliation: tổng doanh thu/profit giữa Excel và `dw.FactSales`.
