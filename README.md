# 🔮 Customer Churn Prediction Pipeline

> **End-to-End Machine Learning System** — Dự báo khách hàng rời bỏ cho doanh nghiệp bán lẻ thực tế

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.3%2B-F7931E?logo=scikit-learn&logoColor=white)](https://scikit-learn.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-1.7%2B-189fdd)](https://xgboost.readthedocs.io)
[![Excel](https://img.shields.io/badge/Microsoft_Excel-Dashboard-217346?logo=microsoftexcel&logoColor=white)](#)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)](https://jupyter.org)

---

## 📌 Tổng Quan Dự Án

Dự án xây dựng một **pipeline Machine Learning hoàn chỉnh** để dự báo khách hàng có nguy cơ rời bỏ (churn) trong tháng tới, phục vụ trực tiếp cho hoạt động vận hành sales của doanh nghiệp bán lẻ.

Toàn bộ quy trình được thiết kế theo tiêu chuẩn **production-ready**: từ làm sạch dữ liệu thực tế từ file Excel nghiệp vụ, xây dựng feature engineering theo thời gian thực, backtest nghiêm ngặt theo time-split, đến xuất danh sách khách hàng có phân tầng rủi ro và hành động cụ thể cho đội sales.

---

## 📊 Dashboard Tổng Quan

### Customer Analytic Dashboard (Excel)

![Customer Analytic Dashboard](assets/customer_dashboard.png)

**Nội dung dashboard:**
- 📦 KPIs: **Doanh thu · Lợi nhuận · Tỷ lệ lợi nhuận · Số đơn hàng · Số khách hàng · Giá trị/đơn**
- 📈 Doanh thu & Lợi nhuận theo Segment khách hàng (VIP / Trung thành / Tiềm năng / Mới / Cần chú ý)
- 🍩 Phân bổ số lượng khách hàng theo phân khúc
- 📊 Số lượng khách hàng theo tháng — theo dõi xu hướng theo phân khúc
- 🏆 Top 5 khách hàng mua nhiều nhất

### Product Analytic Dashboard (Excel)

![Product Analytic Dashboard](assets/product_dashboard.png)

**Nội dung dashboard:**
- 💰 KPIs: Doanh thu · Lợi nhuận · Giá trị/đơn · Tỷ lệ lợi nhuận
- 🥇 Top 5 sản phẩm theo **Tỷ lệ Lợi nhuận %** (Bar + Line combo)
- 📊 Top 10 sản phẩm theo Doanh thu
- 🔵 Revenue Share & LN/Khách theo phân khúc
- 🏢 Doanh thu theo **Ngành Hàng** (12 nhóm sản phẩm)

---

## 🎯 Bài Toán Kinh Doanh

| Mục tiêu | Chi tiết |
|----------|----------|
| **Input** | Dữ liệu giao dịch thực tế từ `ver1.xlsx` (sheet `Data Model`) |
| **Output** | `churn_list.xlsx` — danh sách 159 khách hàng với xác suất churn, mức rủi ro và hành động cụ thể |
| **Định nghĩa Churn** | Khách **không mua hàng** trong cửa sổ 30 ngày mục tiêu |
| **Horizon dự báo** | Tháng tiếp theo (rolling monthly) |
| **Đối tượng dùng** | Đội sales vận hành — phân loại ưu tiên tiếp cận khách |

---

## 🏗️ Kiến Trúc Pipeline

```
ver1.xlsx (Data Model)
        │
        ▼  Clean_data.py
   ┌─────────────────────┐
   │  Data Cleaning      │  Làm sạch, parse ngày, ép kiểu số, phân loại Ngành Hàng
   │  Feature Derivation │  Tỷ lệ LN, Doanh Thu/Đơn vị
   └─────────────────────┘
        │
        ▼  churn_pipeline_main.ipynb
   ┌─────────────────────────────────────────────────────┐
   │  STEP 1: Config (Mode, dates, output paths)         │
   │  STEP 2: Load & Clean (parse dates, numeric coerce) │
   │  STEP 3: Feature Engineering                        │
   │    ├─ Recency (ngày kể từ lần mua cuối)            │
   │    ├─ Frequency (số lần mua trong 90 ngày)         │
   │    ├─ AOV (doanh thu trung bình / đơn)             │
   │    ├─ Margin (tỷ lệ lợi nhuận)                     │
   │    ├─ PromoRate (% khuyến mãi)                     │
   │    ├─ Trend (so sánh DT 30 ngày gần vs 30 ngày cũ)│
   │    ├─ DaysSinceFirst (độ trưởng thành KH)          │
   │    └─ ActiveMonths (số tháng hoạt động)            │
   │  STEP 4A: Benchmark CV (LR vs RF)                  │
   │  STEP 4B: Backtest Time-Split (Sep→Oct train,      │
   │           Nov predict vs Nov thực tế)              │
   │    └─ Weighted Ensemble + Optimal Threshold (F2)   │
   │  STEP 4E: Confusion Matrix Heatmap                  │
   │  STEP 4C: Forecast December (multi-snapshot train) │
   │  STEP 5: Churn Rate Report by Segment              │
   └─────────────────────────────────────────────────────┘
        │
        ▼
   ┌─────────────────────────────────────┐
   │  OUTPUT FILES                       │
   │  ├─ churn_list.xlsx      (scoring)  │
   │  ├─ churn_deepdive_data.xlsx        │
   │  └─ churn_list_DECEMBER_PREMIUM.xlsx│
   └─────────────────────────────────────┘
```

---

## 🧠 ML Methodology

### Feature Engineering

| Feature | Công thức | Ý nghĩa |
|---------|-----------|---------|
| `Recency` | `(as_of_date - last_purchase).days` | Càng cao → nguy cơ churn càng lớn |
| `Frequency` | `count(transactions)` trong 90 ngày | Tần suất mua hàng |
| `AOV` | `Revenue / Frequency` | Giá trị đơn trung bình |
| `Margin` | `Profit / Revenue` | Biên lợi nhuận |
| `PromoRate` | `mean(% SL Khuyến mãi)` | Mức độ phụ thuộc khuyến mãi |
| `Trend` | `DT_30d_gần - DT_30d_cũ` | Xu hướng mua hàng đang tăng hay giảm |
| `DaysSinceFirst` | `(as_of - first_purchase).days` | Độ trưởng thành của khách hàng |
| `ActiveMonths` | `nunique(Month)` | Số tháng có giao dịch |

### Model Selection & Backtest

```
Backtest Strategy: Time-Split nghiêm ngặt (không random split)
  ├─ Train:   Snapshot 30/09 → Label Tháng 10
  ├─ Predict: Snapshot 31/10
  └─ Evaluate vs: Ground truth Tháng 11

Models Benchmarked:
  ├─ Logistic Regression  (C=0.5, class_weight={0:1, 1:2})
  └─ Random Forest        (n=150, max_depth=4, Sigmoid Calibration)
```

### Weighted Ensemble

```python
# Trọng số tính từ Backtest AUC của từng model
weights[model] = AUC_model / sum(AUC_all_models)
ens_prob = Σ (prob[model] * weights[model])
```

### Optimal Threshold (F2-Tuning)

```
Mục tiêu kép:
  1. Tối đa F2-Score (ưu tiên Recall — bắt trọn khách churn)
  2. Không vượt quá 55 khách "Khẩn cấp" (phù hợp năng lực sales)
  3. Recall ≥ 72% tại ngưỡng tối ưu
```

### Phân Tầng Rủi Ro (Output)

| ChurnProb | Mức độ | Hành động |
|-----------|--------|-----------|
| > 0.75 | 🔴 **Khẩn cấp** | Gặp mặt trực tiếp (VIP) / Gọi ngay |
| 0.55 – 0.75 | 🟠 **Cao** | Zalo offer |
| 0.35 – 0.55 | 🟡 **Trung bình** | Theo dõi định kỳ |
| < 0.35 | 🟢 **Thấp** | Không cần can thiệp |

---

## 📈 Kết Quả Đạt Được

### Benchmark CV (TimeSeriesSplit k=3)

| Model | CV AUC | Train AUC |
|-------|--------|-----------|
| Random Forest (Calibrated) | **~0.9835** | — |
| Logistic Regression | ~0.9750 | — |

### Backtest Tháng 11 (Time-Split)

| Metric | Giá trị |
|--------|---------|
| **Ensemble AUC** | Cao — phân tách rõ |
| **XGBoost Churn Rate Error** | `0.000102` (rất sát thực tế) |
| **Random Forest Error** | `0.008781` |
| **Logistic Error** | `0.019460` |

### Deep Dive Instance-Level (Tháng 11)

| | Dự báo: Không Churn | Dự báo: Churn |
|---|---|---|
| **Thực tế: Không Churn** | TN = 49 ✅ | FP = 0 ✅ |
| **Thực tế: Churn** | FN = 0 ✅ | TP = 67 ✅ |

> 💡 **96.55%** xác suất rơi vào vùng phân cực (`≤ 0.2` hoặc `≥ 0.8`) — mô hình phân tách cực kỳ rõ ràng.

### Forecast Tháng 12

| Metric | Giá trị |
|--------|---------|
| Số khách được score | **159 khách** |
| Predicted churn rate | **41.75%** (~66 khách có nguy cơ churn) |
| File vận hành | `churn_list.xlsx` |

---

## 📁 Cấu Trúc Project

```
ver1/
│
├── 📓 churn_pipeline_main.ipynb     # Pipeline chính — chạy từng cell Shift+Enter
├── 🐍 Clean_data.py                 # Data cleaning & feature engineering từ file gốc
├── 📝 CHURN_END_TO_END_SUMMARY.md   # Tóm tắt toàn bộ quá trình & quyết định kỹ thuật
│
├── 📊 ver1.xlsx                     # Dataset gốc (sheet: Data Model)
├── 📊 bc.xls                        # File nghiệp vụ gốc
│
├── 📤 churn_list.xlsx               # OUTPUT: Danh sách churn forecast (tháng 12)
├── 📤 churn_list_DECEMBER_PREMIUM.xlsx  # OUTPUT: Bản premium với thông tin bổ sung
├── 📤 churn_deepdive_data.xlsx      # OUTPUT: Deep-dive confusion matrix data
│
└── 📖 README.md                     # Tài liệu dự án
```

---

## 🚀 Hướng Dẫn Chạy

### Yêu cầu

```bash
pip install pandas numpy scikit-learn xgboost openpyxl matplotlib seaborn jupyter
```

### Chạy Pipeline Chính (Jupyter Notebook)

```bash
# 1. Mở notebook
jupyter lab churn_pipeline_main.ipynb

# 2. Chạy từng cell theo thứ tự (Shift + Enter)
#    STEP 0: Import libraries
#    STEP 1: Config (chọn MODE)
#    STEP 2: Load data
#    STEP 3: Feature engineering utilities
#    STEP 4A: Benchmark
#    STEP 4B: Backtest + Optimal Threshold
#    STEP 4E: Confusion Matrix visualization
#    STEP 4C: Forecast December
#    STEP 5: Rate Report
```

### Config MODE

```python
# Trong STEP 1, thay đổi MODE:
MODE = 'full'          # Chạy toàn bộ pipeline (benchmark → backtest → forecast)
MODE = 'backtest'      # Chỉ chạy backtest tháng 11
MODE = 'forecast_dec'  # Chỉ chạy forecast tháng 12
```

### Chạy Data Cleaning Script

```bash
# Cập nhật INPUT_FILE trong Clean_data.py trước khi chạy
python Clean_data.py
```

> ⚠️ **Lưu ý**: Đóng tất cả file Excel trước khi chạy script để tránh lỗi `PermissionError`.

---

## 🧰 Tech Stack

| Category | Tools |
|----------|-------|
| **Language** | Python 3.10+ |
| **Data Manipulation** | `pandas`, `numpy` |
| **Machine Learning** | `scikit-learn` (Logistic Regression, Random Forest, Pipeline, TimeSeriesSplit, CalibratedClassifierCV) |
| **Gradient Boosting** | `xgboost` (XGBClassifier) |
| **Visualization** | `matplotlib`, `seaborn` |
| **Notebook** | `jupyter`, `jupyterlab` |
| **Excel I/O** | `openpyxl` |
| **Business Dashboard** | Microsoft Excel (Pivot, Slicer, Chart) |

---

## 📐 Thiết Kế & Quyết Định Kỹ Thuật

### Tại sao dùng Time-Split thay vì Random Split?

> Với bài toán dự báo churn theo tháng, **random split gây rò rỉ dữ liệu tương lai** vào tập train. Time-split đảm bảo model chỉ học từ dữ liệu trong quá khứ và predict tương lai — đúng với kịch bản vận hành thực tế.

### Tại sao dùng Weighted Ensemble?

> Mỗi model có điểm mạnh riêng trên từng loại pattern. Ensemble có trọng số dựa trên backtest AUC giúp tận dụng điểm mạnh của cả hai model, đồng thời giảm phương sai so với dùng một model đơn lẻ.

### Tại sao dùng F2-Score để tìm ngưỡng?

> Trong bài toán churn, **False Negative (bỏ sót khách churn) tốn kém hơn False Positive** (tiếp cận nhầm khách không churn). F2-Score ưu tiên Recall gấp đôi Precision, phù hợp với mục tiêu kinh doanh.

### Tại sao Calibrate Random Forest?

> Random Forest thường cho xác suất không calibrated (dồn về 0 và 1). `CalibratedClassifierCV(method='sigmoid')` đảm bảo `ChurnProb = 0.7` thực sự có nghĩa là 70% nguy cơ churn — cho phép phân tầng rủi ro có ý nghĩa.

---

## 📋 Ghi Chú Vận Hành

- **Cadence đánh giá mô hình:** Backtest theo tháng với time-split — không dùng random split.
- **Cập nhật dữ liệu:** Thêm giao dịch mới vào sheet `Data Model` của `ver1.xlsx`, cập nhật `AS_OF_TRAIN` và chạy lại pipeline.
- **Output file bị khóa:** Đóng file Excel trước khi chạy script; nếu bị khóa, script sẽ tự động xuất file `*_new.xlsx`.

---

*Dự án thực tế — Dữ liệu bán lẻ 2023 · Built with Python, scikit-learn & Excel*
