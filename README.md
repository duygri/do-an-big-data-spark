# Pipeline nhập dữ liệu taxi vàng New York bằng PySpark

Dự án môn **Nhập dữ liệu lớn**, xây dựng pipeline batch để nhập và xử lý dữ liệu chuyến taxi vàng tại New York bằng Python và Apache Spark (PySpark). Pipeline đọc dữ liệu CSV qua Bronze, làm sạch sang Silver, tạo đặc trưng chuyến đi, bổ sung nhãn khu vực đón/trả và ghi Parquet đã xử lý vào Gold.

> **Trạng thái:** Pipeline xử lý đúng sáu tệp taxi tháng 01–06/2020, tạo Bronze, Silver, báo cáo chất lượng, feature taxi, zone enrichment tùy chọn, processed Gold và bảy bảng tổng hợp; báo cáo Markdown, CSV và biểu đồ được sinh cùng lần chạy.

## Mục tiêu đề tài

- Thực hành nhập dữ liệu dạng bảng dung lượng lớn bằng Spark thay vì xử lý toàn bộ bằng bộ nhớ của một tiến trình Python đơn lẻ.
- Áp dụng schema tường minh, lưu dữ liệu trung gian ở định dạng Parquet và kiểm tra kết quả sau khi ghi.
- Làm sạch các trường bắt buộc, loại bản ghi trùng hoàn toàn và tạo báo cáo chất lượng có thể kiểm tra lại.
- Chuẩn bị dữ liệu cho các phép tổng hợp theo thời gian, khu vực đón/trả và đặc điểm chuyến đi.

### Câu hỏi phân tích

- Số chuyến thay đổi như thế nào theo tháng, ngày trong tuần và giờ trong ngày?
- Những khu vực nào có nhiều chuyến đón hoặc trả nhất?
- Quãng đường, tiền cước, tiền tip và phương thức thanh toán phân bố ra sao?
- Các chỉ số taxi thay đổi như thế nào trong sáu tháng đầu năm 2020?

Pipeline tạo kết quả cho các câu hỏi trên trên dữ liệu tháng 01–06/2020. Phạm vi này không bao gồm so sánh với năm 2019.

## Dữ liệu

Nguồn dữ liệu là bộ [New York Yellow Taxi Trip Data trên Kaggle](https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019). Bộ Kaggle có 18 tệp CSV theo tháng từ 01/2019 đến 06/2020; pipeline của repo chỉ đọc sáu tệp từ **01/2020 đến 06/2020**. Dữ liệu do NYC Taxi & Limousine Commission (TLC) công bố; xem thêm [trang dữ liệu chuyến đi của TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Các trường được pipeline sử dụng gồm thời điểm đón/trả, số hành khách, quãng đường, mã khu vực đón/trả, loại thanh toán và các khoản cước/phụ phí. Schema hiện khai báo 18 cột trong `src/ingestion/schema.py`. Nếu có `taxi+_zone_lookup.csv`, pipeline dùng bảng này để bổ sung tên khu vực; nếu không có, pipeline vẫn chạy với mã khu vực. Dữ liệu shapefile chưa được đọc.

Tải dữ liệu từ Kaggle, giải nén đúng sáu tệp `yellow_tripdata_2020-01.csv` đến `yellow_tripdata_2020-06.csv` vào `data/raw/nyc_taxi/`. Cấu hình mặc định chỉ chọn các tháng này; các tệp 2019 khác có thể nằm cùng thư mục nhưng không khớp glob đầu vào. Dữ liệu thô và dữ liệu sinh ra không được đưa vào Git.

## Pipeline

```text
6 tệp CSV (01–06/2020)
   │
   ▼
data/raw/nyc_taxi/
   │  PySpark đọc CSV theo schema taxi vàng
   ▼
data/bronze/raw/       Parquet (Snappy), kiểm tra schema và số dòng
   │
   ▼
data/silver/cleaned/   Làm sạch và kiểm tra chất lượng
   ├── reports/generated/taxi_quality.json
   └── reports/generated/taxi_quality.md
   │
   ▼
data/gold/taxi_trips/  Feature engineering + zone enrichment (tùy chọn), Parquet theo năm/tháng đón
data/gold/aggregations/  Bảy bảng
reports/generated/taxi_aggregations/  CSV, báo cáo Markdown và biểu đồ tổng hợp
```

### Các bước làm sạch hiện có

- Loại dòng thiếu thời điểm đón/trả hoặc mã khu vực đón/trả.
- Chỉ loại bản ghi trùng khi cả 18 trường đều giống nhau; dữ liệu nguồn không có mã chuyến ổn định.
- Chuẩn hóa `store_and_fwd_flag` bằng cách bỏ khoảng trắng và chuyển về chữ thường.
- Thống kê giá trị null, một số thống kê số học và các điều kiện bất thường. Các cảnh báo được ghi vào báo cáo; bản ghi bị cảnh báo vẫn được giữ trong Silver để tránh tự ý loại dữ liệu.

Bronze và Silver được ghi đè khi chạy lại pipeline. Báo cáo chứa số dòng theo từng giai đoạn, schema, thống kê null, min/max/average và số lượng các cảnh báo chất lượng.

## Công nghệ

- Python 3.10+
- Apache Spark / PySpark 3.5.x
- Java 11 hoặc Java 17
- Parquet với nén Snappy
- Matplotlib để tạo biểu đồ từ các bảng tổng hợp nhỏ
- Pandas và psutil cho benchmark so sánh engine
- PyYAML để đọc cấu hình YAML

## Cài đặt và chạy

### 1. Cài môi trường

Cài Python 3.10 trở lên và Java 11 hoặc 17. Tạo môi trường ảo và cài các thư viện:

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Nếu dùng macOS/Linux, kích hoạt môi trường ảo bằng `source .venv/bin/activate`.

### 2. Chuẩn bị dữ liệu và cấu hình

1. Tải bộ dữ liệu từ Kaggle và giải nén sáu tệp tháng 01–06/2020 vào `data/raw/nyc_taxi/`.
2. Sao chép cấu hình mẫu:

   ```powershell
   Copy-Item configs/config.example.yaml configs/config.yaml
   ```

3. Cấu hình mặc định chọn `yellow_tripdata_2020-0[1-6].csv`. Pipeline yêu cầu đủ đúng sáu tệp tháng 01–06/2020 trước khi Spark khởi động; không trỏ `input.path` tới một tệp đơn hoặc các tháng khác.

### 3. Chạy pipeline

Chạy từ thư mục gốc của repository:

```powershell
python -m src.pipeline.main --config configs/config.yaml
```

Các đường dẫn trong cấu hình được tính từ thư mục hiện hành. Pipeline hiện chạy ở chế độ local bằng `local[*]`; có thể thay đổi Spark master và một số tuỳ chọn trong file cấu hình.

Pipeline cũng ghi bảy bảng Gold vào `paths.aggregations` (mặc định `data/gold/aggregations`) và report vào `paths.analysis_reports` (mặc định `reports/generated/taxi_aggregations`). `aggregation.top_n` (mặc định `10`) chỉ giới hạn số khu vực trên biểu đồ; Parquet và CSV giữ mọi khu vực. Chi tiết grain, cột, quy tắc null và biểu đồ nằm trong [docs/aggregations.md](docs/aggregations.md).

Benchmark persistence Spark chạy riêng, không nằm trong pipeline thường ngày:

```powershell
python scripts/benchmark_spark_aggregation.py `
  --input data/gold/taxi_trips `
  --output reports/generated/aggregation_benchmark `
  --repetitions 3
```

## Kết quả đầu ra

| Đường dẫn | Nội dung |
| --- | --- |
| `data/bronze/raw/` | Dữ liệu sau khi đọc CSV, lưu Parquet và xác minh lại số dòng/schema. |
| `data/silver/cleaned/` | Dữ liệu sau các bước làm sạch hiện có. |
| `reports/generated/taxi_quality.json` | Báo cáo chất lượng dạng JSON. |
| `reports/generated/taxi_quality.md` | Tóm tắt báo cáo chất lượng dạng Markdown. |
| `data/gold/taxi_trips/` | Chuyến taxi đã tạo đặc trưng và bổ sung nhãn khu vực, phân vùng theo năm/tháng đón. |
| `data/gold/aggregations/` | Bảy bảng Parquet: theo tháng, weekday, giờ, zone đón/trả, thống kê metric và payment mix. |
| `reports/generated/taxi_aggregations/csv/` | Một CSV có header cho mỗi bảng tổng hợp. |
| `reports/generated/taxi_aggregations/taxi_aggregation_report.md` | Định nghĩa metric, phạm vi ngày và quy tắc dữ liệu. |
| `reports/generated/taxi_aggregations/charts/` | `monthly_trips.png`, top pickup/dropoff zones và `payment_mix.png`. |

## Cấu trúc repository

```text
configs/                 Cấu hình mẫu cho pipeline
data/                    Các thư mục raw, bronze, silver và gold
docs/                    Tài liệu kiến trúc, nhập liệu và quy tắc chất lượng
notebooks/               Vị trí dự kiến cho khám phá dữ liệu và demo
reports/generated/       Báo cáo chất lượng được sinh khi chạy pipeline
src/
├── ingestion/            Đọc dữ liệu, schema và ghi Bronze
├── cleaning/             Làm sạch, kiểm tra và ghi Silver
├── pipeline/             Điểm chạy pipeline
├── transformation/       Tạo đặc trưng chuyến đi, nối bảng taxi zone và ghi Gold
└── aggregation/          Bảng tổng hợp taxi, Gold/report exports và benchmark Spark
tests/integration/        Kiểm thử tích hợp bước nhập liệu và làm sạch
scripts/                  Vị trí cho các script hỗ trợ
```

## Kiểm thử

Chạy các kiểm thử trong repository bằng:

```powershell
pytest -q
```

## Tài liệu liên quan

- [Kiến trúc pipeline](docs/architecture.md)
- [Chi tiết bước nhập dữ liệu và Bronze](docs/ingestion.md)
- [Quy tắc làm sạch và báo cáo chất lượng](docs/cleaning-quality.md)
- [Định nghĩa feature taxi](docs/transformation-features.md)
- [Zone enrichment](docs/transformation-joins.md)
- [Processed Parquet](docs/processed-output.md)
- [Taxi aggregations và báo cáo](docs/aggregations.md)
- [Benchmark Spark và Pandas](docs/benchmarking.md)
- [Bộ dữ liệu trên Kaggle](https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
