# Customer Churn Prediction Pipeline

Pipeline dự báo khách hàng có nguy cơ churn cho dữ liệu bán lẻ trong `ver1.xlsx`.

## Trạng thái hiện tại

- Input chính: `data/ver1.xlsx`, sheet `Data Model`
- Dữ liệu: 705 giao dịch, 159 khách hàng, 220 mã sản phẩm
- Khoảng thời gian dữ liệu: 2023-09-01 đến 2023-11-30
- Output vận hành chính: `outputs/churn_list.xlsx`
- Model được chọn sau benchmark mới nhất: `random_forest_calibrated`
- Snapshot forecast: 2023-11-30, dự báo rủi ro churn tháng 12

## Cấu trúc

| File | Vai trò |
|---|---|
| `churn_pipeline_main.py` | Pipeline chính: load data, build feature, benchmark model, forecast, export Excel |
| `data/ver1.xlsx` | Workbook nguồn và dashboard Excel |
| `Clean_data.py` | Script làm sạch/tái dựng workbook Excel khi cần |
| `outputs/churn_list.xlsx` | Danh sách khách hàng cần xử lý theo rủi ro churn |
| `outputs/churn_model_comparison.xlsx` | Kết quả benchmark Logistic Regression, Random Forest, XGBoost |
| `outputs/churn_backtest_nov.xlsx` | Backtest chi tiết tháng 11 theo từng khách hàng |
| `outputs/churn_deepdive_data.xlsx` | Confusion matrix và metric tóm tắt theo model |
| `outputs/churn_rate_report.xlsx` | Báo cáo churn theo segment |
| `outputs/model_selection_summary.json` | Metadata lựa chọn model và threshold |
| `requirements.txt` | Dependency cần cài để chạy lại pipeline |
| `dw/DW_IMPLEMENTATION_PLAN.md` | Plan DW mới bám sát pipeline hiện tại |
| `dw/DW_Master_Script_Full.sql` | DDL/SP SQL Server cho DW |
| `dw/load_excel_to_sql.py` | Load `data/ver1.xlsx` vào `stg.SalesRaw` |
| `dw/load_scores_to_sql.py` | Load `outputs/churn_list.xlsx` vào `stg.ChurnScoreRaw` |

## Cách chạy

```bash
pip install -r requirements.txt
python churn_pipeline_main.py
```

Trong môi trường Codex hiện tại, dependency đã được cài cục bộ vào `.venv/Lib/site-packages`, nên có thể chạy bằng:

```bash
C:\Users\usr\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe churn_pipeline_main.py
```

## Feature Engineering

Pipeline tạo feature theo cửa sổ lookback 90 ngày tại từng snapshot:

| Feature | Ý nghĩa |
|---|---|
| `Recency` | Số ngày từ lần mua cuối tới snapshot |
| `Frequency` | Số giao dịch trong 90 ngày gần nhất |
| `AOV` | Doanh thu trung bình mỗi giao dịch |
| `PromoRate` | Tỷ lệ khuyến mãi trung bình |
| `Margin` | Lợi nhuận / doanh thu |
| `Trend` | Doanh thu 30 ngày gần nhất trừ 30 ngày trước đó |
| `DaysSinceFirst` | Số ngày từ giao dịch đầu tiên trong lookback |
| `ActiveMonths` | Số tháng có phát sinh giao dịch trong lookback |
| `Segment` | Phân khúc khách hàng, đưa vào model bằng one-hot encoding |

## Backtest và chọn model

Do dữ liệu hiện chỉ có 3 tháng, pipeline dùng một holdout theo thời gian:

- Train: snapshot 2023-09-30, label theo tháng 10
- Test: snapshot 2023-10-31, label theo tháng 11
- Forecast: snapshot 2023-11-30, dự báo tháng 12

Kết quả benchmark mới nhất:

| Model | ROC AUC | Average Precision | Precision | Recall | F2 | TN | FP | FN | TP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Random Forest calibrated | 0.9304 | 0.9595 | 0.9455 | 0.7761 | 0.8050 | 46 | 3 | 15 | 52 |
| XGBoost | 0.9240 | 0.9435 | 0.9434 | 0.7463 | 0.7788 | 46 | 3 | 17 | 50 |
| Logistic Regression | 0.7999 | 0.7577 | 0.8158 | 0.9254 | 0.9012 | 35 | 14 | 5 | 62 |

Random Forest calibrated được chọn vì có ROC AUC và Average Precision tốt nhất, đồng thời đạt precision cao với giới hạn danh sách khẩn cấp khoảng 55 khách trong backtest. Logistic Regression có recall cao hơn nhưng tạo quá nhiều khách cần xử lý ngay, phù hợp làm baseline hơn là model vận hành.

## Output vận hành

`outputs/churn_list.xlsx` hiện gồm 159 khách hàng với các cột:

- `Khách hàng`, `Segment`
- `Recency`, `Frequency`, `AOV`, `Margin`, `Trend`, `ActiveMonths`
- `RiskScore`, `ChurnProb`, `Mức độ`, `Hành động`

Phân bổ rủi ro output mới nhất:

| Mức độ | Số khách |
|---|---:|
| Khẩn cấp | 66 |
| Cao | 25 |
| Trung bình | 16 |
| Thấp | 52 |

## Lưu ý chất lượng

- Backtest chỉ có một split theo tháng vì dữ liệu nguồn mới có 3 tháng. Khi có thêm tháng mới, nên mở rộng rolling backtest trước khi chốt lại model.
- Dữ liệu có một số dòng lợi nhuận âm; hiện pipeline giữ nguyên vì có thể là nghiệp vụ hợp lệ, nhưng nên kiểm tra trong vận hành.
- Output legacy `churn_list_DECEMBER_PREMIUM.xlsx` đã được gỡ khỏi project; `outputs/churn_list.xlsx` là bản vận hành chính.

## Data Warehouse

Phần DW nằm trong thư mục `dw/`.

Runbook ngắn:

```bash
pip install -r requirements.txt
```

```sql
-- chạy trong SQL Server sau khi tạo database ChurnDW
:r dw/DW_Master_Script_Full.sql
```

Nếu chạy bằng `sqlcmd`, dùng UTF-8 input để giữ đúng tiếng Việt trong stored procedure:

```bash
sqlcmd -S . -E -C -f 65001 -i dw/DW_Master_Script_Full.sql
```

```bash
set CHURN_DW_CONN_STR=Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=ChurnDW;Trusted_Connection=yes;TrustServerCertificate=yes;
python dw/load_excel_to_sql.py
python dw/load_scores_to_sql.py
```

Sau đó có thể gọi:

```sql
EXEC dw.sp_ETL_RunAll;
EXEC dw.sp_FE_BuildCustomerSnapshot @SnapshotDate = '2023-11-30';
EXEC dw.sp_Score_LoadFromStaging @SnapshotDate = '2023-11-30';
EXEC dw.sp_Report_ChurnList @SnapshotDate = '2023-11-30';
```
