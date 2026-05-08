# Churn Modeling End-to-End Summary

## Kết luận sau lần chuẩn hóa

Pipeline hiện đã được gom về một entrypoint chính: `churn_pipeline_main.py`.

Model phù hợp nhất trong lần benchmark mới nhất là **Random Forest calibrated**, không phải XGBoost. XGBoost có kết quả rất gần, nhưng Random Forest calibrated nhỉnh hơn trên ROC AUC và Average Precision trong holdout tháng 11.

## Dữ liệu

- Nguồn: `data/ver1.xlsx`, sheet `Data Model`
- Giao dịch: 705 dòng
- Khách hàng: 159
- Mã sản phẩm: 220
- Thời gian: 2023-09-01 đến 2023-11-30
- Không có null hoặc duplicate trong bảng nguồn

## Thiết kế backtest

- Snapshot train: 2023-09-30
- Label train: khách có/không mua trong tháng 10
- Snapshot test: 2023-10-31
- Label test: khách có/không mua trong tháng 11
- Snapshot forecast: 2023-11-30, dự báo churn tháng 12

Định nghĩa churn: `1` nếu khách không mua trong cửa sổ 30 ngày/tháng kế tiếp, `0` nếu có mua.

## Feature

Pipeline dùng cửa sổ lookback 90 ngày:

- `Segment`
- `Recency`
- `Frequency`
- `AOV`
- `PromoRate`
- `Margin`
- `Trend`
- `DaysSinceFirst`
- `ActiveMonths`

## Kết quả benchmark

| Model | ROC AUC | Average Precision | Precision | Recall | F2 | Confusion Matrix |
|---|---:|---:|---:|---:|---:|---|
| Random Forest calibrated | 0.9304 | 0.9595 | 0.9455 | 0.7761 | 0.8050 | TN=46, FP=3, FN=15, TP=52 |
| XGBoost | 0.9240 | 0.9435 | 0.9434 | 0.7463 | 0.7788 | TN=46, FP=3, FN=17, TP=50 |
| Logistic Regression | 0.7999 | 0.7577 | 0.8158 | 0.9254 | 0.9012 | TN=35, FP=14, FN=5, TP=62 |

## Quyết định model

Chọn `random_forest_calibrated`.

Lý do:

- AUC cao nhất trong holdout tháng 11.
- Average Precision cao nhất.
- Precision cao, ít false positive hơn Logistic Regression.
- Danh sách khách hàng khẩn cấp trong backtest được kiểm soát tốt hơn cho năng lực sales.

Logistic Regression bắt được nhiều churn hơn nhưng đẩy quá nhiều khách vào nhóm cần xử lý, nên hiện phù hợp làm baseline. XGBoost là ứng viên mạnh thứ hai và nên tiếp tục theo dõi khi có thêm dữ liệu tháng mới.

## Output đã xuất lại

- `outputs/churn_list.xlsx`: danh sách churn forecast tháng 12, 159 khách hàng
- `outputs/churn_model_comparison.xlsx`: bảng benchmark 3 model
- `outputs/churn_backtest_nov.xlsx`: xác suất/prediction từng khách hàng trong backtest tháng 11
- `outputs/churn_deepdive_data.xlsx`: metric và confusion matrix theo model
- `outputs/churn_rate_report.xlsx`: churn summary theo segment
- `outputs/model_selection_summary.json`: metadata lựa chọn model

## Lưu ý vận hành

- Chỉ có một monthly holdout vì dữ liệu hiện có 3 tháng; không nên diễn giải metric như cam kết production dài hạn.
- Khi có thêm dữ liệu tháng 12 trở đi, nên chạy rolling backtest nhiều tháng và so lại Random Forest với XGBoost.
- Output legacy `churn_list_DECEMBER_PREMIUM.xlsx` đã được gỡ khỏi project; `outputs/churn_list.xlsx` là source of truth.
