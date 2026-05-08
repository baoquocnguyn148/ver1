# Kiến Trúc Data Warehouse & MLOps Pipeline

Tài liệu này mô tả chi tiết toàn bộ kiến trúc Data Warehouse (DW) được thiết kế chuyên biệt cho Hệ thống Dự báo Churn (Customer Churn Prediction), kết hợp hoàn hảo giữa phương pháp **ELT (Extract, Load, Transform)** và **Reverse ETL**.

---

## 1. Tổng Quan Hệ Thống (Architecture Overview)

Hệ thống được chia làm 3 cụm chính:

1. **Nguồn Dữ liệu (Source):** Dữ liệu bán hàng từ file Excel (`data/ver1.xlsx`).
2. **Cơ sở dữ liệu (Data Warehouse - SQL Server):** Nơi làm sạch, biến đổi (Transform), lưu trữ (Storage) và áp dụng các Business Rules.
3. **Mô hình Trí tuệ nhân tạo (ML Pipeline - Python):** Nơi đọc Feature đã biến đổi từ DW để học (Train) và dự báo (Predict) xác suất Churn.

### 🌊 Luồng Dữ Liệu Chính (Data Flow)

```text
[1. EXCEL] data/ver1.xlsx
      │
      ├─(Python: load_excel_to_sql.py) ──> [2. SQL STAGING] stg.SalesRaw
                                                │
                                                └─(SQL: sp_ETL_RunAll) ──> [3. STAR SCHEMA] dw.DimCustomer, dw.DimProduct, dw.FactSales
                                                                                │
                                                                                └─(SQL: sp_FE_BuildCustomerSnapshot) ──> [4. FEATURE STORE] dw.CustomerSnapshot
                                                                                                                                │
[5. MACHINE LEARNING] Python XGBoost / Random Forest <──────────────────────────────────────────────────────────────────────────┘
      │
      ├─(Python: load_scores_to_sql.py) ──> [6. SQL STAGING] stg.ChurnScoreRaw
                                                │
                                                └─(SQL: sp_Score_LoadFromStaging) ──> [7. SCORING FACT] dw.FactChurnScore
                                                                                            │
[8. POWER BI / BUSINESS] <──(SQL: sp_Report_ChurnList / sp_Score_ApplyBusinessRules) <──────┘
```

---

## 2. Các Schema trong Database

Hệ thống tuân thủ thiết kế chuẩn Enterprise với 4 Schema tách biệt:

| Schema | Ý nghĩa | Vai trò thực tế |
|---|---|---|
| **`stg`** | **Staging (Vùng đệm)** | Chứa dữ liệu đổ trực tiếp từ Python vào. Dữ liệu tại đây có thể bị xóa/Truncate. Có các cột `RowHash` và `SourceRowNumber` để chống trùng lặp. |
| **`dw`** | **Data Warehouse** | Chứa hệ thống Core Star Schema (Dim, Fact) và các bảng Feature (CustomerSnapshot). |
| **`cfg`** | **Configuration** | Lưu trữ tham số hệ thống (LookbackDays, Churn Thresholds) và phiên bản Model (Model Version). |
| **`audit`** | **Audit Trail** | Ghi log toàn bộ các lần chạy ETL (Bắt đầu mấy giờ, Lỗi gì, Bơm bao nhiêu dòng). Có `LoadBatchId` gán vào từng dòng dữ liệu để dễ dàng truy xuất (Traceability). |

---

## 3. Data Dictionary (Từ Điển Dữ Liệu)

Dưới đây là chi tiết chức năng của các bảng vật lý trong cơ sở dữ liệu:

### 3.1. Schema `stg` (Staging Area)
- **`stg.SalesRaw`**: Bảng hứng dữ liệu thô (raw) từ file Excel. Mỗi lần chạy ETL, Python sẽ nạp dữ liệu vào đây. Cột `RowHash` dùng để phát hiện sự thay đổi nội dung (CDC).
- **`stg.ChurnScoreRaw`**: Bảng hứng kết quả dự báo từ file Excel do Python ML xuất ra. Giữ nguyên điểm xác suất thô (`ChurnProb`).

### 3.2. Schema `dw` (Core Data Warehouse)
- **`dw.DimDate`**: Bảng Dimension Thời gian (Ngày, Tháng, Năm, Quý). Phục vụ Power BI slice & dice dữ liệu báo cáo chuẩn hóa.
- **`dw.DimCustomer`**: Bảng Dimension Khách hàng. Lưu trữ thông tin định danh (`CustomerName`), phân khúc (`Segment`), và theo dõi ngày mua đầu/cuối của khách.
- **`dw.DimProduct`**: Bảng Dimension Sản phẩm. Lưu mã hàng, tên hàng và phân loại ngành hàng (`CategoryName`).
- **`dw.FactSales`**: Bảng Fact trung tâm (Heart of DW). Lưu trữ mọi giao dịch bán hàng. Chứa các khóa ngoại liên kết tới `Dim` và các metric kinh doanh (`Revenue`, `Profit`). Dùng `SourceRowNumber` chống nhân đôi dữ liệu.
- **`dw.CustomerSnapshot`**: Bảng Feature Store phục vụ trực tiếp cho Machine Learning. Lưu giữ hành vi khách hàng tại một thời điểm (`SnapshotDate`) với 8 biến RFM (Recency, Frequency, AOV, Trend...).
- **`dw.FactChurnScore`**: Bảng Fact Scoring. Chứa điểm rủi ro rụng khách (`ChurnProb`) do AI chấm. Đặc biệt lưu trữ `ModelVersion` và tự động mapping ra hành động thực thi (`SalesAction`) cho phòng Kinh doanh.

### 3.3. Schema `cfg` & `audit` (Operation & Logging)
- **`cfg.DwParameter`**: Bảng cấu hình hệ thống (Key-Value). Lưu các ngưỡng rủi ro (`UrgentThreshold = 0.538...`) và chu kỳ tính toán (`LookbackDays = 90`).
- **`cfg.ModelVersion`**: Sổ đăng ký Model (Model Registry). Lưu lịch sử các mô hình AI đã train kèm theo performance metrics (ROC-AUC, F2).
- **`audit.EtlBatch`**: Bảng nhật ký hệ thống. Ghi nhận chi tiết mỗi lần chạy ELT (`LoadBatchId`, thời gian, số dòng nạp, trạng thái lỗi/thành công).

---

## 4. Quy trình Data Engineering (ELT)

Thay vì dùng Python để Transform dữ liệu, dự án tận dụng sức mạnh tính toán của SQL Server (ELT - Load trước, Transform sau).

### Bước 4.1: Ingestion (Nạp dữ liệu)
- Python script `load_excel_to_sql.py` đọc file Excel, làm sạch cơ bản (Clean nulls, datetime, map tiếng Việt sang English ASCII), tính mã băm (RowHash) và đẩy tốc độ cao (`fast_executemany`) vào `stg.SalesRaw`.

### Bước 4.2: Transform to Star Schema
Các Stored Procedure chạy tuần tự để đẩy data từ `stg` sang `dw`:
1. `dw.sp_ETL_LoadDimDate`: Tự sinh danh sách ngày tháng.
2. `dw.sp_ETL_LoadDimensions`: Tự lọc ra danh sách Khách hàng (`DimCustomer`) và Sản phẩm (`DimProduct`) duy nhất. Tracking được ngày mua hàng đầu/cuối của khách.
3. `dw.sp_ETL_LoadFactSales`: Nạp dữ liệu vào bảng Sự kiện (Fact). Dùng `SourceRowNumber` để đảm bảo thao tác là **Idempotent** (chạy lại bao nhiêu lần cũng không bị nhân đôi dữ liệu).

### Bước 4.3: In-Database Feature Engineering
Stored Procedure `dw.sp_FE_BuildCustomerSnapshot` là trái tim của hệ thống Data Engineering:
- Nó chạy theo `SnapshotDate` (ví dụ: ngày cuối tháng).
- Quét lùi lại `LookbackDays` (ví dụ 90 ngày) theo thông số lấy từ bảng `cfg.DwParameter`.
- Dùng Window Functions và Group By để tính toán ra 8 biến RFM cực kỳ phức tạp: `Recency`, `Frequency`, `AOV`, `Margin`, `PromoRate`, `Trend` (Chênh lệch doanh thu 30 vs 60 ngày), `ActiveMonths`.
- Lưu kết quả vào `dw.CustomerSnapshot` sẵn sàng cho AI học.

---

## 5. MLOps & Reverse ETL (Vận hành Mô hình)

Khi AI dự đoán xong, ta không để kết quả "chết" ở file Excel, mà đẩy ngược vào DB (Reverse ETL) thông qua `load_scores_to_sql.py`.

### Điểm nhấn MLOps (Machine Learning Operations):
1. **Dynamic Thresholds (Ngưỡng động):** Thay vì hard-code mức `0.75` là Khẩn cấp, bảng `cfg.DwParameter` lưu mức chuẩn xác suất tối ưu F2-Score (ví dụ: `0.538...`). Stored Procedure `sp_Score_ApplyBusinessRules` sẽ tự động đọc bảng Config này để quyết định Risk Level.
2. **Model Versioning:** Bảng `cfg.ModelVersion` lưu lại toàn bộ các Model từng được train (Random Forest, XGBoost) kèm theo AUC, F2 Score. Bảng `FactChurnScore` lưu điểm số có đính kèm theo `ModelVersion`, giúp Data Scientist dễ dàng làm A/B Testing hoặc phân tích lịch sử rụng khách của từng Model.
3. **Automated Sales Actions:** Ngay khi điểm AI rớt xuống Database, Rule Engine của SQL tự động gán nhãn: `Gặp mặt trực tiếp`, `Gọi ngay` hoặc `Zalo Offer` tùy thuộc vào độ Khẩn cấp và Phân khúc KH.

---

## 6. Hướng dẫn Vận hành (Runbook)

Hệ thống được thiết kế hoàn toàn tự động dựa trên Environment Variables.

**Chạy toàn bộ luồng Data Pipeline (Từ Excel lên SQL):**
```bash
set CHURN_DW_RUN_ETL=1
python dw/load_excel_to_sql.py
```

**Chạy AI Model (Train/Predict):**
```bash
python churn_pipeline_main.py
```

**Đẩy kết quả AI ngược vào DB & Kích hoạt Business Rules:**
```bash
set CHURN_DW_APPLY_SCORE=1
python dw/load_scores_to_sql.py
```

*(Lưu ý: Trên macOS/Linux hoặc Git Bash, thay `set` bằng `export`)*

---
*Tài liệu này đóng vai trò là "Bản vẽ kỹ thuật" cho bất kỳ Data Engineer / Data Scientist nào muốn duy trì và mở rộng hệ thống Churn Prediction Data Warehouse này trong tương lai.*
