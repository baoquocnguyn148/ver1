# 🚀 Kế Hoạch Triển Khai Data Warehouse (DW Implementation Plan)

*Tài liệu này đóng vai trò là "bản thiết kế thi công" (blueprint) cho toàn bộ hệ thống Data Warehouse. Nội dung đã được tối ưu hóa để thân thiện với người đọc (human-friendly) và bám sát tuyệt đối vào thực tế dự án Data Science hiện hành.*

**Thông tin cấu hình hiện tại:**
- **Source of truth dữ liệu giao dịch:** `data/ver1.xlsx` (sheet `Data Model`)
- **Pipeline ML hiện hành:** `churn_pipeline_main.py`
- **Model vận hành đã benchmark:** `Random Forest Calibrated`
- **Output vận hành (Reverse ETL):** `outputs/churn_list.xlsx`
- **Hệ quản trị CSDL mục tiêu:** Microsoft SQL Server

---

## 🎯 1. Mục Tiêu Cốt Lõi Của DW

Hệ thống Data Warehouse (DW) **không sinh ra để thay thế Python ML**. Thay vào đó, DW và Python được kết hợp để phát huy thế mạnh của từng công cụ:
- **DW đảm nhiệm:** Trở thành Single Source of Truth, chuẩn hóa dữ liệu bán hàng, lưu trữ Feature Snapshot phục vụ AI, quản lý Score History và cung cấp dữ liệu cho Reporting (Power BI).
- **Python đảm nhiệm:** Chuyên sâu vào thuật toán Học máy (Train, Tuning, Predict).

**🌊 Luồng dữ liệu chuẩn (Standard Data Flow):**
```text
[Excel Source] data/ver1.xlsx
      │
      ├─> 📥 [STG] stg.SalesRaw (Staging Area)
      │
      ├─> 🗄️ [DW] dw.DimDate, dw.DimCustomer, dw.DimProduct, dw.FactSales (Star Schema)
      │
      ├─> 🧠 [DW] dw.CustomerSnapshot (Feature Store for ML)
      │
      ├─> 🤖 [PYTHON] Random Forest Calibrated (Machine Learning Pipeline)
      │
      ├─> 📤 [STG] stg.ChurnScoreRaw (Reverse ETL Staging)
      │
      ├─> 🎯 [DW] dw.FactChurnScore (Scoring History)
      │
      └─> 📊 [REPORTING] Power BI / Stored Procedures 
```

---

## 💡 2. Nguyên Tắc Thiết Kế (Core Design Principles)

Để đảm bảo hệ thống vận hành trơn tru và dễ bảo trì, các nguyên tắc sau được tuân thủ nghiêm ngặt:

1. **Chuẩn hóa Định dạng:** Tuyệt đối không dùng tên cột tiếng Việt trong Core DW. Các trường được chuyển đổi sang chuẩn Tiếng Anh/ASCII để đảm bảo tương thích 100% với các công cụ Data Stack khác.
2. **Bảo toàn Dấu vết Dữ liệu (Traceability):** Vùng Staging giữ lại toàn bộ mapping từ file Excel. Cột `STT` gốc được chuyển thành `SourceRowNumber` để truy xuất nguồn gốc và chống trùng lặp.
3. **SSOT (Single Source of Truth):** Không tự tạo logic phân loại `Ngành Hàng` bằng `CASE WHEN` trong SQL. DW sử dụng trực tiếp Master Data từ Source để tránh sai lệch cấu trúc Category.
4. **Nhất quán Feature Engineering (A.I Consistency):** Logic tính toán Feature bằng SQL phải khớp hoàn toàn với bản Python `churn_pipeline_main.py`:
   - Cửa sổ quan sát (Lookback window) là **90 ngày**.
   - Công thức lùi ngày: `SaleDate > SnapshotDate - 90` và `SaleDate <= SnapshotDate`.
   - Chỉ giữ lại Snapshot của những khách hàng có phát sinh giao dịch trong lookback window.
   - `Trend = (Doanh thu 30 ngày gần nhất) - (Doanh thu 30 ngày trước đó)`.
5. **Chống Rò rỉ Dữ liệu (No Data Leakage):** Trạng thái phân khúc (Historical Segment) phải được chốt chính xác tại ngày chạy Snapshot, tuyệt đối không dùng Segment của thì tương lai.
6. **Đồng bộ Business Rules:** Logic phân loại Rủi ro (Risk Rule) và Hành động (Sales Action) phải ánh xạ chính xác từ mô hình:
   - `UrgentThreshold` = 0.538... (Mức tối ưu F2-Score)
   - VIP + Rủi ro Khẩn cấp ➡️ **Gặp mặt trực tiếp**
   - Rủi ro Khẩn cấp hoặc Bỏ dở lâu (Recency > 60) ➡️ **Gọi ngay**
   - Rủi ro Cao hoặc Recency > 30 ➡️ **Zalo offer**
   - Các trường hợp còn lại ➡️ **Theo dõi định kỳ**

---

## 🏗️ 3. Kiến Trúc Schemas

Thiết kế Database tách biệt rõ ràng theo chuẩn Enterprise Data Warehouse:

| Schema | Vai trò & Chức năng |
|:---:|---|
| **`stg`** | **Staging (Vùng đệm):** Nơi hạ cánh dữ liệu từ Excel và kết quả Scoring từ Python. |
| **`dw`** | **Data Warehouse (Lõi):** Chứa hệ thống Core Star Schema (Dim, Fact), Feature Snapshot và Reporting Views. |
| **`cfg`** | **Configuration (Cấu hình):** Quản lý Model Version, các ngưỡng rủi ro Thresholds, và Parameter hệ thống. |
| **`audit`**| **Audit Log (Nhật ký):** Ghi nhận trạng thái Load (Batch metadata, Thành công/Thất bại, Số lượng row nạp). |

---

## 🗂️ 4. Các Bảng Dữ Liệu Trung Tâm (Core Tables)

### 📥 `stg.SalesRaw`
Hứng dữ liệu thô từ `data/ver1.xlsx`. Tên cột được Mapping chuẩn Tiếng Anh.
- *Fields:* `SourceRowNumber`, `ProductCode`, `ProductName`, `Revenue`, `SaleDate`, `CustomerName`, `SegmentName`, `Profit`, `MarginPct`, `CategoryName`, v.v.
- *Metadata:* `SourceFileName`, `LoadBatchId`, `RowHash` (Dùng cho CDC).

### 🛍️ `dw.FactSales`
Bảng Fact giao dịch trung tâm (Granularity: 1 dòng giao dịch từ Excel).
- *Unique Key:* `(SourceSystem, SourceRowNumber)`.
- *Note:* Lưu trữ trường `SegmentAtSale` để phục vụ tái tạo Historical Snapshot mà không dính Data Leakage.

### 🧠 `dw.CustomerSnapshot`
Bảng Feature Store trực tiếp cung cấp đạn dược cho Machine Learning.
- *Features:* `Segment`, `Recency`, `Frequency`, `AOV`, `PromoRate`, `Margin`, `Trend`, `DaysSinceFirst`, `ActiveMonths`.
- *ML Target:* `ChurnLabel` (Phục vụ huấn luyện & backtest).
- *Primary Key:* `(SnapshotDate, CustomerKey)`.

### 🎯 `dw.FactChurnScore`
Bảng lưu trữ Lịch sử Điểm số (Scoring History) theo chiều không gian (Khách hàng) và chiều thời gian (Snapshot).
- *Fields:* `ChurnProb`, `RiskScore`, `RiskLevel`, `SalesAction`.
- *MLOps Fields:* `ModelName`, `ModelVersion`.

---

## ⚙️ 5. Hệ Thống Data Engineering Engine (Stored Procedures)

Hệ thống được tự động hóa bằng SQL Stored Procedures mạnh mẽ:

| Stored Procedure | Phân nhóm | Mục đích hoạt động |
|---|---|---|
| `cfg.sp_SetDefaultConfig` | **Config** | Khởi tạo cấu hình hệ thống & Thresholds mặc định. |
| `audit.sp_StartEtlBatch` / `sp_FinishEtlBatch` | **Audit** | Mở / Đóng một phiên làm việc ETL (Batch Management). |
| `dw.sp_ETL_LoadDimDate` / `LoadDimensions` | **ELT** | Xử lý và Upsert các bảng Dimension (Thời gian, Khách hàng, Sản phẩm). |
| `dw.sp_ETL_LoadFactSales` | **ELT** | Merge dữ liệu giao dịch từ Staging vào bảng Fact. |
| `dw.sp_ETL_RunAll` | **ELT Master** | Thủ tục Orchestrator: Chạy tự động toàn bộ luồng ELT. |
| `dw.sp_FE_BuildCustomerSnapshot` | **Feature Eng.** | Tính toán 8 biến số RFM phức tạp trong cửa sổ 90 ngày. |
| `dw.sp_FE_BuildChurnLabel` | **Feature Eng.** | Gắn nhãn Churn (Label 0/1) phục vụ quá trình Retrain/Backtest. |
| `dw.sp_Score_UpsertChurnScore` | **MLOps** | Reverse ETL: Cập nhật Churn Score từ AI vào Data Warehouse. |
| `dw.sp_Score_ApplyBusinessRules` | **MLOps** | Ứng dụng Business Rules động để gán mức rủi ro và hành động thực thi. |

---

## 🐍 6. Tích Hợp Python & MLOps

Bộ công cụ Python đảm nhiệm khâu cầu nối (Ingestion & Reverse ETL):
- **`dw/load_excel_to_sql.py`**: Quét file `ver1.xlsx` và nạp vào `stg.SalesRaw`.
- **`dw/load_scores_to_sql.py`**: Quét file kết quả `outputs/churn_list.xlsx` từ mô hình AI và nạp vào `stg.ChurnScoreRaw`.

**Biến Môi Trường (Environment Variables):**
- `CHURN_DW_CONN_STR`: Connection string kết nối cơ sở dữ liệu.
- `CHURN_DW_SOURCE_FILE`: Tùy chỉnh nguồn dữ liệu Excel đầu vào (nếu có).
- `CHURN_DW_SCORE_FILE`: Tùy chỉnh nguồn file kết quả (nếu có).

---

## 📝 7. Sổ Tay Vận Hành (Runbook)

Quy trình chuẩn hóa một chu kỳ vận hành từ A đến Z:

**1. Khởi tạo Cơ sở dữ liệu:** Tạo DB tên `ChurnDW` và chạy master script `dw/DW_Master_Script_Full.sql`.
**2. Nạp dữ liệu nguồn:** Chạy script Python Ingestion.
```bash
python dw/load_excel_to_sql.py
```
**3. Data Engineering In-DB (ETL & FE):** Khởi chạy thủ tục thông qua SQL:
```sql
EXEC dw.sp_ETL_RunAll @LoadBatchId = NULL;
EXEC dw.sp_FE_BuildCustomerSnapshot @SnapshotDate = '2023-11-30';
```
**4. Kích hoạt Trí Tuệ Nhân Tạo:** Huấn luyện / Dự báo bằng Python.
```bash
python churn_pipeline_main.py
```
**5. Nạp ngược Kết quả (Reverse ETL):**
```bash
python dw/load_scores_to_sql.py
```
**6. Gán Business Rules & Xuất Báo Cáo:**
```sql
EXEC dw.sp_Score_LoadFromStaging @SnapshotDate = '2023-11-30';
EXEC dw.sp_Report_ChurnList @SnapshotDate = '2023-11-30';
```

---

## 🛡️ 8. Chiến Lược Đưa Lên Môi Trường Thực Tế (Production Hardening)

Để hệ thống thực sự vươn tầm Enterprise, các cấu phần sau sẽ được siết chặt:
1. **Lưu vết Vĩnh viễn (Audit Trail History):** Lưu toàn bộ lịch sử Churn Score qua các tháng thay vì ghi đè (overwrite), cho phép đánh giá chất lượng model qua thời gian.
2. **Rolling Backtest Multi-months:** Xây dựng quy trình Backtesting nhiều kỳ liên tiếp bằng cách tự động sinh Label trên bảng `dw.CustomerSnapshot`.
3. **Automated Model Registry:** Tự động tăng tiến `cfg.ModelVersion` mỗi khi có Model mới chiến thắng (Champion Model) trong quá trình re-train.
4. **Orchestrator Automation:** Tích hợp toàn bộ Data Pipeline (từ Ingestion đến Scoring) vào công cụ điều phối (Airflow / SQL Agent) theo lịch trình hàng tháng.
5. **Data Reconciliation:** Bổ sung thủ tục đối soát tự động (ví dụ: Tổng doanh thu/Lợi nhuận tại `stg.SalesRaw` phải bằng 100% so với `dw.FactSales`).
