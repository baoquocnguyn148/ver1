# Churn Modeling End-to-End Summary

## 1) Mục tiêu đã thực hiện

- Xây dựng pipeline dự báo churn từ `ver1.xlsx`.
- So sánh nhiều thuật toán (Logistic Regression, Random Forest, XGBoost).
- Backtest theo time-split đúng kịch bản:
  - Train bằng dữ liệu đến hết tháng 10 (tháng 9-10)
  - Predict và đối chiếu với thực tế tháng 11
- Deep-dive sai số ở level cá nhân (instance-level).
- Retrain mô hình để dự báo tương lai tháng 12.
- Xuất `churn_list.xlsx` hoàn thiện cho vận hành.

## 2) Dữ liệu và định nghĩa bài toán

- Nguồn dữ liệu: `ver1.xlsx` (sheet `Data Model`)
- Cột dùng cho modeling:
  - `Khách hàng`, `Ngày Xuất`, `Doanh Thu`, `Lợi Nhuận`, `% SL Khuyến mãi`, `Segment`
- Feature chính:
  - `Recency`, `Frequency`, `AOV`, `PromoRate`, `Margin`, `Segment`
- Định nghĩa nhãn churn:
  - `churn = 1` nếu khách **không mua** trong cửa sổ 30 ngày mục tiêu
  - `churn = 0` nếu có mua

## 3) Quy trình triển khai đã làm

### Bước A - Baseline pipeline churn

- Script: `build_churn_model.py`
- Thực hiện:
  - load + clean dữ liệu
  - build feature theo mốc thời gian
  - train model baseline (ban đầu Logistic Regression)
  - scoring khách hàng
  - xuất:
    - `churn_list.xlsx`
    - `churn_rate_report.xlsx`

### Bước B - So sánh thuật toán

- Script: `compare_churn_models.py`
- Models đã benchmark:
  - Logistic Regression
  - Random Forest
  - XGBoost
- Output:
  - `churn_model_comparison.xlsx`

### Bước C - Backtest time split chuẩn

- Script: `backtest_time_split.py`
- Kịch bản:
  - Train trên thông tin đến `2023-10-31`
  - Predict churn tháng 11
  - So với ground truth tháng 11
- Output:
  - `churn_backtest_nov.xlsx`

### Bước D - Deep dive sai số + forecast tháng 12

- Script: `xgb_deepdive_and_december_forecast.py`
- Phần 1 (Deep dive tháng 11):
  - Confusion matrix
  - Probability distribution (kiểm tra mô hình có "vân phân" quanh 0.5 hay không)
- Phần 2 (Forecast tháng 12):
  - Retrain XGBoost theo multi-snapshot:
    - Snapshot 30/09 -> label theo tháng 10
    - Snapshot 31/10 -> label theo tháng 11
  - Score tại 30/11 để dự báo churn tháng 12
  - Cập nhật `churn_list.xlsx`
- Output:
  - `churn_deepdive_and_december.xlsx`
  - `churn_list.xlsx` (bản forecast mới nhất)

## 4) Kết quả đạt được

## 4.1 So sánh model (benchmark)

- Random Forest: CV AUC ~ `0.9835`
- XGBoost: CV AUC ~ `0.9825`
- Logistic Regression: CV AUC ~ `0.9750`

Giai đoạn benchmark tổng quát cho thấy RF nhỉnh nhẹ về CV AUC.

## 4.2 Backtest đúng kịch bản tháng 11

- Kịch bản: train 9-10, predict 11, đối chiếu 11 thật.
- XGBoost cho kết quả tốt nhất trong backtest này.
- Sai số churn rate tháng 11 (predicted vs actual):
  - XGBoost abs error ~ `0.000102` (rất sát)
  - Random Forest abs error ~ `0.008781`
  - Logistic abs error ~ `0.019460`

## 4.3 Deep dive (instance-level) XGBoost tháng 11

- Confusion matrix:
  - `TP=67`, `TN=49`, `FP=0`, `FN=0`
- Probability distribution:
  - Tỷ lệ xác suất trong vùng `0.4-0.6`: `0.00%`
  - Tỷ lệ phân cực (`<=0.2` hoặc `>=0.8`): `96.55%`

Kết luận deep dive: mô hình phân tách rất rõ trên backtest này, không bị dồn quanh ngưỡng 0.5.

## 4.4 Forecast tháng 12 (bản vận hành hiện tại)

- Model dùng: XGBoost (sau retrain multi-snapshot)
- Số khách được score: `159`
- Predicted churn rate tháng 12 (mean probability): `0.4175` (~`41.75%`)
- File vận hành: `churn_list.xlsx`

## 5) Quyết định đã chốt

- **Model triển khai chính:** `XGBoost`
  - Lý do: tốt nhất trên backtest time-split tháng 11 (sai số churn rate thấp nhất) và deep-dive cho thấy phân tách rõ.
- **Cách train cho forecast vận hành:** multi-snapshot
  - Snapshot `30/09` -> label theo tháng 10
  - Snapshot `31/10` -> label theo tháng 11
  - Score tại `30/11` để dự báo tháng 12.
- **Ngưỡng phân tầng rủi ro để hành động:**
  - `ChurnProb > 0.8` -> `Khẩn cấp`
  - `0.6 < ChurnProb <= 0.8` -> `Cao`
  - Còn lại -> `Trung bình`
- **Rule hành động sales:**
  - `Recency > 60` -> Gọi ngay
  - `30 < Recency <= 60` -> Zalo offer
  - VIP + xác suất cao -> Ưu tiên gặp trực tiếp
- **Cadence đánh giá mô hình:**
  - Duy trì backtest theo tháng với time-split (không dùng random split cho bài toán này).

## 6) File chính nên giữ

- `ver1.xlsx` (data gốc dùng modeling)
- `churn_pipeline_main.py` (file code chính đã hợp nhất toàn bộ bước)
- `churn_pipeline_main.ipynb` (bản notebook, mỗi step một cell để chạy `Shift+Enter`)
- `churn_list.xlsx` (output churn mới nhất)
- `churn_rate_report.xlsx` (report tổng hợp churn rate)
- `churn_model_comparison.xlsx` (kết quả benchmark thuật toán)
- `churn_backtest_nov.xlsx` (kết quả backtest tháng 11)
- `churn_deepdive_and_december.xlsx` (deep dive + dec forecast)
- `CHURN_END_TO_END_SUMMARY.md` (file tổng hợp duy nhất)

## 7) Ghi chú vận hành

- Khi chạy script xuất Excel, nếu file đang mở trong Excel có thể bị khóa ghi.
- Đảm bảo đóng file đích trước khi chạy để tránh lỗi `PermissionError`.
