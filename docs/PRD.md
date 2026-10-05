# PRD: Pipeline nhập dữ liệu taxi vàng New York bằng PySpark

## 1. Thông tin tài liệu

| Trường | Giá trị |
|---|---|
| **Tên tài liệu** | Product Requirements Document — Pipeline nhập dữ liệu taxi vàng New York bằng PySpark |
| **Phiên bản** | 0.2 |
| **Tác giả** | Nhóm dự án (PM phụ trách: cần bổ sung) |
| **Trạng thái** | Đã đồng bộ với phạm vi 6 tệp 2020 và pipeline hiện tại |
| **Cập nhật lần cuối** | 05/10/2026 |

## 2. Tổng quan và vấn đề cần giải quyết

Dự án xây dựng pipeline batch để nhập và xử lý dữ liệu chuyến taxi vàng tại New York bằng Python và Apache Spark. Bộ dữ liệu Kaggle có các tệp CSV theo tháng từ 01/2019 đến 06/2020, nhưng pipeline của dự án chỉ đọc đúng sáu tệp từ tháng 01 đến tháng 06/2020.

Xử lý dữ liệu nhiều dòng bằng pandas hoặc Python thuần có thể vượt quá giới hạn bộ nhớ của một máy. Dự án sử dụng PySpark, schema tường minh và Parquet để thực hành xử lý phân tán và kiểm tra được kết quả qua từng bước.

Pipeline hiện đọc sáu CSV, ghi Bronze, làm sạch sang Silver và tạo báo cáo chất lượng. Pipeline cũng tạo feature taxi, bổ sung zone nếu có lookup, ghi processed Gold, bảy bảng aggregation Gold và các báo cáo CSV/Markdown/biểu đồ.

## 3. Mục tiêu và mục đích

### Mục tiêu học phần

- Thực hành nhập và xử lý dữ liệu taxi bằng Spark.
- Áp dụng schema tường minh gồm 18 cột.
- Lưu các lớp trung gian và kết quả bằng Parquet nén Snappy.
- Thiết kế bước làm sạch, kiểm tra chất lượng và đối soát số dòng.
- Tạo bảng tổng hợp phục vụ phân tích mô tả trong phạm vi sáu tháng đầu năm 2020.

### Mục tiêu sản phẩm

1. Chạy pipeline Raw → Bronze → Silver → Gold bằng cấu hình.
2. Chỉ nhận đúng sáu CSV từ tháng 01 đến tháng 06/2020; từ chối đầu vào thiếu hoặc lệch phạm vi trước khi khởi tạo Spark.
3. Xác minh schema và số dòng sau khi ghi Bronze, Silver và các bảng Gold.
4. Tạo báo cáo chất lượng, báo cáo aggregation và biểu đồ cho mỗi lần chạy thành công.
5. Ghi rõ phạm vi thời gian và quy tắc xử lý để người đọc diễn giải đúng kết quả.

## 4. Chỉ số thành công / KPI

| Chỉ số | Mục tiêu | Trạng thái |
|---|---|---|
| Phạm vi dữ liệu | Đúng sáu CSV từ 2020-01 đến 2020-06; preflight từ chối tháng thiếu hoặc tệp ngoài phạm vi trong glob | Đã triển khai |
| Tính toàn vẹn schema | Đọc theo schema taxi vàng gồm 18 cột | Đã triển khai |
| Xác minh Bronze/Silver | Đọc lại Parquet và đối soát schema, số dòng | Đã triển khai |
| Báo cáo chất lượng | Tạo báo cáo JSON và Markdown sau lần chạy thành công | Đã triển khai |
| Kết quả Gold | Tạo bảy bảng aggregation taxi cùng CSV, báo cáo Markdown và biểu đồ | Đã triển khai |
| Hiệu năng | Có script benchmark Spark persistence và Spark/Pandas; baseline phụ thuộc máy chạy | Đã có công cụ; chưa đặt ngưỡng |

## 5. Người dùng và bên liên quan

| Vai trò | Nhu cầu |
|---|---|
| **Sinh viên / nhóm phát triển** | Chạy pipeline trên sáu tệp, kiểm tra kết quả từng lớp và tái lập báo cáo |
| **Giảng viên** | Đánh giá cách nhập dữ liệu lớn, làm sạch, đối soát và phân tích bằng Spark |
| **Người phân tích dữ liệu** | Xem xu hướng chuyến đi, khu vực, quãng đường, cước, tip và phương thức thanh toán trong 01–06/2020 |

## 6. Phạm vi

### Trong phạm vi

- Đọc đúng sáu CSV taxi vàng tháng 01–06/2020 bằng cấu hình mặc định.
- Áp dụng schema tường minh gồm 18 cột.
- Ghi Bronze, Silver, processed Gold và bảy bảng aggregation dưới dạng Parquet.
- Làm sạch dữ liệu và tạo báo cáo chất lượng.
- Tạo feature thời gian/chuyến đi và nối zone lookup tùy chọn.
- Xuất các bảng aggregation thành CSV, báo cáo Markdown và biểu đồ.
- Cung cấp benchmark độc lập cho Spark persistence và Spark/Pandas.

### Ngoài phạm vi

- Đọc các tháng năm 2019 hoặc tháng 07/2020 trở đi trong pipeline này.
- So sánh năm 2019 với năm 2020.
- Xử lý streaming hoặc thời gian thực.
- Xây dựng dashboard hoặc ứng dụng cho người dùng cuối.
- Dự báo, machine learning hoặc kết luận quan hệ nhân quả.
- Loại bản ghi chỉ vì có giá trị bất thường; cảnh báo chất lượng không tự động loại dữ liệu.
- Cung cấp cụm Spark hoặc dịch vụ production.

## 7. User stories và trường hợp sử dụng

### User stories

- **Là sinh viên**, tôi muốn chạy pipeline trên đúng sáu tệp từ tháng 01 đến tháng 06/2020 để tạo các lớp dữ liệu có thể kiểm tra.
- **Là người vận hành**, tôi muốn biết trước khi Spark khởi động nếu thiếu tệp hoặc glob chọn sai tháng.
- **Là người kiểm tra dữ liệu**, tôi muốn xem số dòng bị loại và các cảnh báo chất lượng.
- **Là người phân tích**, tôi muốn xem tổng hợp chuyến theo thời gian, khu vực và đặc điểm chuyến đi trong kỳ dữ liệu.
- **Là giảng viên**, tôi muốn kiểm tra schema, quy tắc làm sạch, đầu ra và số liệu đối soát.

### Trường hợp sử dụng chính

1. Người dùng tải bộ dữ liệu và đặt sáu CSV 2020-01 đến 2020-06 vào thư mục Raw; các CSV 2019 khác không được chọn bởi glob mặc định.
2. Người dùng sao chép cấu hình mẫu và điều chỉnh đường dẫn cục bộ khi cần.
3. Preflight xác nhận đúng sáu tệp trước khi tạo Spark session.
4. Pipeline đọc CSV theo schema, ghi Bronze, làm sạch và ghi Silver cùng báo cáo chất lượng.
5. Pipeline tạo feature, xử lý zone lookup nếu có, ghi processed Gold, bảy bảng tổng hợp và các báo cáo đầu ra.

## 8. Yêu cầu chức năng

Các trạng thái dưới đây phân biệt **hiện trạng** với **yêu cầu cho phiên bản mục tiêu**.

### Must-have

#### FR-01 — Đọc CSV theo schema

**Trạng thái:** Đã triển khai.

Pipeline phải đọc CSV có header theo schema taxi vàng 18 cột.

**Tiêu chí nghiệm thu**

- Lần chạy pipeline yêu cầu đúng sáu tệp tháng 01–06/2020 theo glob cấu hình; preflight kiểm tra bộ tệp trước khi Spark khởi động.
- Schema đầu vào phải khớp với schema taxi vàng mà pipeline hỗ trợ.
- Đường dẫn không khớp tệp nào phải khiến lần chạy báo lỗi rõ ràng.
- Dữ liệu không hợp lệ theo chế độ đọc `FAILFAST` phải làm lần chạy thất bại thay vì âm thầm bỏ qua bản ghi.
- Pipeline phải ghi log schema và một mẫu nhỏ của dữ liệu đầu vào.

#### FR-02 — Ghi và xác minh Bronze

**Trạng thái:** Đã triển khai.

Pipeline phải ghi dữ liệu đầu vào sang Parquet nén Snappy ở `data/bronze/raw/`.

**Tiêu chí nghiệm thu**

- Thư mục Bronze được ghi theo chế độ overwrite.
- Pipeline đọc lại dữ liệu vừa ghi.
- Số dòng và schema sau khi đọc lại phải bằng số dòng và schema đầu vào.
- Nếu số dòng hoặc schema không khớp, pipeline phải báo lỗi và không tiếp tục xử lý Silver.

#### FR-03 — Làm sạch và ghi Silver

**Trạng thái:** Đã triển khai.

Pipeline phải áp dụng các quy tắc làm sạch hiện có và ghi kết quả sang Parquet tại `data/silver/cleaned/`.

**Tiêu chí nghiệm thu**

- Loại bản ghi thiếu thời điểm đón, thời điểm trả, mã khu vực đón hoặc mã khu vực trả.
- Chỉ loại bản ghi trùng khi toàn bộ 18 trường giống nhau.
- Chuẩn hóa `store_and_fwd_flag`: bỏ khoảng trắng đầu/cuối, chuyển chữ thường; chuỗi rỗng thành null.
- Giữ các giá trị null ở trường không bắt buộc và giữ các bản ghi có cảnh báo bất thường.
- Báo cáo thể hiện số dòng trước và sau từng bước làm sạch.
- Sau khi ghi Silver, pipeline đọc lại và xác minh schema, số dòng.

#### FR-04 — Xuất báo cáo chất lượng

**Trạng thái:** Đã triển khai.

Sau lần chạy thành công, pipeline phải tạo:

- `reports/generated/taxi_quality.json`
- `reports/generated/taxi_quality.md`

**Tiêu chí nghiệm thu**

- Báo cáo gồm schema, số dòng theo giai đoạn, số dòng bị loại do thiếu trường bắt buộc và trùng chính xác.
- Báo cáo gồm số lượng null trước và sau làm sạch.
- Báo cáo gồm min, max và average cho các trường số được cấu hình.
- Báo cáo gồm số lượng bản ghi vi phạm từng kiểm tra chất lượng.
- Với mỗi loại cảnh báo, báo cáo có tối đa hai bản ghi ví dụ nếu có vi phạm.
- Báo cáo JSON đọc được bằng parser JSON tiêu chuẩn; báo cáo Markdown có thể đọc trực tiếp.

#### FR-05 — Chạy pipeline bằng cấu hình

**Trạng thái:** Đã triển khai.

Người dùng phải có thể chạy pipeline từ thư mục gốc bằng:

```powershell
python -m src.pipeline.main --config configs/config.yaml
```

**Tiêu chí nghiệm thu**

- Cấu hình mẫu có thể được sao chép thành cấu hình cục bộ.
- Người dùng có thể đổi đường dẫn đầu vào, định dạng, header, đường dẫn Bronze/Silver/report, Spark master và giới hạn partition được cấu hình.
- Có thể đổi đường dẫn đầu vào, nhưng glob sau khi đổi vẫn phải chọn đúng sáu tệp tháng 01–06/2020.
- Thiếu file cấu hình hoặc cấu hình sai phải khiến chương trình kết thúc với mã lỗi và ghi thông tin lỗi vào log.
- Các đường dẫn tương đối được tính từ thư mục hiện hành khi chạy.

#### FR-06 — Lưu số liệu và đối soát cho Gold

**Trạng thái:** Đã triển khai; các bảng tổng hợp được ghi Parquet và kiểm tra lại schema, số dòng.

Mỗi bảng Gold phải có grain rõ ràng và được lưu ở định dạng phù hợp để tiếp tục truy vấn bằng Spark.

**Tiêu chí nghiệm thu**

- Mỗi bảng mô tả rõ đơn vị của một dòng, các khóa nhóm và chỉ số được tính.
- Tổng số chuyến trong bảng tổng hợp thời gian đối soát được với tập dữ liệu Silver áp dụng cùng điều kiện lọc.
- Quy tắc loại hoặc giữ bản ghi bất thường được ghi cùng tài liệu Gold.
- Chạy lại cùng đầu vào và cùng cấu hình tạo kết quả logic nhất quán.

#### FR-07 — Tổng hợp theo thời gian

**Trạng thái:** Đã triển khai; có bảng tổng hợp tháng, weekday và giờ đón.

Gold phải hỗ trợ xem số chuyến theo tháng, ngày trong tuần và giờ trong ngày.

**Tiêu chí nghiệm thu**

- Có kết quả tổng hợp số chuyến theo tháng.
- Có kết quả tổng hợp số chuyến theo ngày trong tuần.
- Có kết quả tổng hợp số chuyến theo giờ trong ngày.
- Tên cột và grain của từng bảng được mô tả trong tài liệu.
- Các giá trị thời gian được tạo từ timestamp đón; múi giờ và cách xử lý timestamp được ghi rõ.

#### FR-08 — Tổng hợp theo khu vực đón/trả

**Trạng thái:** Đã triển khai; bảng zone giữ mã, nhãn lookup khi có, số chuyến và thứ hạng xác định.

Gold phải hỗ trợ xếp hạng mã khu vực đón và trả theo số chuyến.

**Tiêu chí nghiệm thu**

- Có bảng riêng cho số chuyến theo `PULocationID` và `DOLocationID`.
- Mỗi kết quả có mã khu vực và số chuyến.
- Quy tắc xử lý mã khu vực không hợp lệ được xác định trước khi tổng hợp.
- Nếu lookup chưa được nối, kết quả ghi rõ đang hiển thị mã thay vì tên khu vực.

#### FR-09 — Tổng hợp đặc điểm chuyến đi và thanh toán

**Trạng thái:** Đã triển khai; thống kê mô tả distance/fare/tip và cơ cấu payment có mẫu số rõ ràng.

Gold phải hỗ trợ mô tả quãng đường, tiền cước, tiền tip và cơ cấu phương thức thanh toán.

**Tiêu chí nghiệm thu**

- Với `trip_distance`, `fare_amount` và `tip_amount`, kết quả có số quan sát hợp lệ và các thống kê mô tả được thống nhất.
- Với `payment_type`, kết quả có số chuyến theo mã thanh toán.
- Nếu hiển thị tỷ lệ phần trăm, mẫu số và cách xử lý giá trị null được nêu rõ.
- Nếu dùng histogram hoặc nhóm khoảng, biên nhóm và đơn vị được ghi lại; không tự diễn giải nhóm là phân loại chính thức của nguồn.

#### FR-10 — Tổng hợp kỳ tháng 01–06/2020

**Trạng thái:** Đã triển khai.

Các bảng Gold và báo cáo phải phản ánh dữ liệu chuyến từ sáu tháng đầu năm 2020.

**Tiêu chí nghiệm thu**

- Bảng theo tháng thể hiện các tháng có dữ liệu trong năm 2020.
- Báo cáo ghi phạm vi ngày thực tế được tổng hợp.
- Không tạo hoặc mô tả kết quả so sánh 2019–2020 khi pipeline không đọc dữ liệu 2019.

#### FR-11 — Kiểm tra bộ tệp đầu vào

**Trạng thái:** Đã triển khai.

Preflight phải xác nhận glob chọn đúng sáu CSV từ tháng 01 đến tháng 06/2020 trước khi tạo Spark session.

**Tiêu chí nghiệm thu**

- Danh sách kỳ vọng gồm `yellow_tripdata_2020-01.csv` đến `yellow_tripdata_2020-06.csv`.
- Tệp thiếu, glob rỗng, định dạng không phải CSV, thư mục thay vì file hoặc tệp tháng ngoài phạm vi trong glob khiến lần chạy báo lỗi rõ ràng.
- CSV 2019 hoặc tháng ngoài 01–06/2020 có thể nằm trong thư mục Raw nhưng không được glob mặc định chọn.

### Should-have

#### FR-12 — Nối lookup khu vực tùy chọn

**Trạng thái:** Đã triển khai tùy chọn.

Pipeline đọc lookup taxi zone khi có cấu hình hoặc tệp lookup theo vị trí mặc định; nếu không có lookup, kết quả vẫn giữ mã khu vực.

**Tiêu chí nghiệm thu**

- Lookup được nối riêng cho khu vực đón và trả.
- Số bản ghi không khớp và khóa lookup trùng được ghi nhận.
- Khi lookup không có, báo cáo khu vực dùng mã thay cho tên.

## 9. Yêu cầu phi chức năng

| Nhóm | Yêu cầu |
|---|---|
| **Khả năng mở rộng** | Xử lý dữ liệu bằng Spark DataFrame; không thu thập toàn bộ dữ liệu thô về driver. Chỉ thu thập mẫu nhỏ hoặc kết quả tổng hợp có giới hạn. |
| **Hiệu năng** | Pipeline phải chạy được trên bộ dữ liệu mục tiêu trong môi trường môn học. Thời gian chạy và cấu hình tài nguyên phải được đo trước khi đặt SLO cụ thể. |
| **Độ tin cậy** | Lỗi đọc, ghi hoặc xác minh phải làm lần chạy thất bại rõ ràng; không báo thành công khi một stage chưa hoàn tất. |
| **Khả năng tái lập** | Cấu hình, schema và quy tắc làm sạch được lưu trong repository; kết quả cùng đầu vào và cùng cấu hình nhất quán về mặt logic. |
| **Khả năng quan sát** | Log thể hiện stage hiện tại, đường dẫn liên quan, schema và số dòng quan trọng. Báo cáo chất lượng được lưu riêng khỏi dữ liệu lớn. |
| **Khả năng bảo trì** | Các bước ingestion, cleaning, transformation và aggregation được tách module để kiểm thử và thay đổi độc lập. |
| **Tương thích** | Môi trường mục tiêu: Python 3.10+, PySpark 3.5.x, Java 11 hoặc 17. |
| **Quản lý dữ liệu** | Không commit dữ liệu thô, Parquet sinh ra hoặc báo cáo sinh ra vào Git. |

## 10. Yêu cầu dữ liệu và schema

### Nguồn và phạm vi

- Bộ dữ liệu: New York Yellow Taxi Trip Data trên Kaggle; bộ có thêm các tháng năm 2019.
- Nguồn gốc: NYC Taxi & Limousine Commission (TLC).
- Phạm vi pipeline: đúng sáu tệp `yellow_tripdata_2020-01.csv` đến `yellow_tripdata_2020-06.csv`.
- Cấu hình mặc định dùng glob `yellow_tripdata_2020-0[1-6].csv`, vì vậy CSV 2019 khác trong thư mục không được đọc.
- Định dạng đầu vào: CSV có header.
- Schema taxi vàng gồm 18 cột; con số này là số trường mỗi dòng, không phải số lượng tệp.
- Bảng tra cứu khu vực là đầu vào tùy chọn.

### Schema hiện tại

Tất cả trường hiện được khai báo nullable trong `src/ingestion/schema.py`.

| Tên cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| `VendorID` | Integer | Mã nhà cung cấp dữ liệu |
| `tpep_pickup_datetime` | Timestamp | Thời điểm đón |
| `tpep_dropoff_datetime` | Timestamp | Thời điểm trả |
| `passenger_count` | Integer | Số hành khách được ghi nhận |
| `trip_distance` | Double | Quãng đường, đơn vị dặm |
| `RatecodeID` | Integer | Mã loại cước |
| `store_and_fwd_flag` | String | Cờ lưu và chuyển tiếp |
| `PULocationID` | Integer | Mã khu vực đón |
| `DOLocationID` | Integer | Mã khu vực trả |
| `payment_type` | Integer | Mã phương thức thanh toán |
| `fare_amount` | Double | Tiền cước |
| `extra` | Double | Khoản phụ phí |
| `mta_tax` | Double | Thuế MTA |
| `tip_amount` | Double | Tiền tip ghi nhận |
| `tolls_amount` | Double | Phí cầu đường |
| `improvement_surcharge` | Double | Phụ phí cải thiện |
| `total_amount` | Double | Tổng tiền |
| `congestion_surcharge` | Double | Phụ phí ùn tắc |

Các khoản tiền được giữ theo đơn vị nguồn là USD. Timestamp và quãng đường được giữ theo kiểu và đơn vị nguồn; không tự chuyển đổi đơn vị trong bước làm sạch hiện có.

## 11. Kiến trúc và luồng pipeline

### Hiện trạng

Sáu CSV tháng 01–06/2020 → Bronze Parquet → Silver Parquet và báo cáo chất lượng → taxi features → zone enrichment tùy chọn → processed Gold Parquet → bảy bảng aggregation Parquet, CSV, báo cáo Markdown và biểu đồ.

Pipeline hiện có đầy đủ các bước trên. Tất cả phép tổng hợp dùng bộ dữ liệu đã làm sạch và feature-engineer; pipeline không đọc CSV năm 2019.

## 12. Quy tắc chất lượng và xác thực dữ liệu

### 12.1 Kiểm tra đầu vào

- Cấu hình mặc định chọn `yellow_tripdata_2020-0[1-6].csv`.
- Preflight yêu cầu đúng sáu file: `yellow_tripdata_2020-01.csv` đến `yellow_tripdata_2020-06.csv`.
- Tệp thiếu, glob rỗng, định dạng không phải CSV, phần tử khớp không phải file hoặc tên ngoài phạm vi khiến pipeline dừng trước khi tạo Spark session.
- CSV phải có header theo cấu hình và schema taxi vàng 18 trường.
- Lỗi parse dữ liệu không hợp lệ phải làm pipeline thất bại theo chế độ `FAILFAST`.
- Trường số để trống có thể trở thành null; không tự điền bằng 0 hoặc trung bình.

### 12.2 Quy tắc làm sạch hiện có

| Quy tắc | Xử lý |
|---|---|
| Null ở thời điểm đón, thời điểm trả, mã khu vực đón hoặc mã khu vực trả | Loại bản ghi |
| Trùng bản ghi | Chỉ loại nếu toàn bộ 18 trường giống nhau |
| Null ở trường không bắt buộc | Giữ lại và thống kê |
| `store_and_fwd_flag` có khoảng trắng | Bỏ khoảng trắng đầu/cuối |
| `store_and_fwd_flag` viết hoa/thường không đồng nhất | Chuyển về chữ thường |
| `store_and_fwd_flag` là chuỗi rỗng | Chuyển thành null |
| Giá trị bất thường theo các kiểm tra bên dưới | Giữ lại trong Silver và ghi cảnh báo |

### 12.3 Điều kiện cảnh báo hiện có

Pipeline hiện thống kê số bản ghi vi phạm các điều kiện sau:

- Thời điểm đón sau thời điểm trả.
- `PULocationID` nhỏ hơn hoặc bằng 0.
- `DOLocationID` nhỏ hơn hoặc bằng 0.
- `passenger_count` âm.
- `trip_distance` âm.
- `fare_amount` âm.
- `total_amount` âm.
- `store_and_fwd_flag` khác `y` hoặc `n`, nếu giá trị không null.

Các điều kiện cảnh báo không đồng nghĩa với quy tắc loại dữ liệu. Ví dụ, khoản cước âm vẫn được giữ vì có thể là điều chỉnh.

### 12.4 Nội dung báo cáo chất lượng

Báo cáo JSON và Markdown phải thể hiện:

- Schema dữ liệu Silver.
- Số dòng Bronze, sau lọc null bắt buộc, sau loại trùng và tại Silver.
- Số dòng bị loại do thiếu trường bắt buộc.
- Số dòng bị loại do trùng toàn dòng.
- Null count trước và sau làm sạch cho các cột.
- Min/max/average cho các trường số được cấu hình.
- Số dòng vi phạm từng điều kiện cảnh báo.
- Tối đa hai ví dụ cho mỗi loại cảnh báo có vi phạm.
- Ghi chú rằng nguồn không có khóa chuyến ổn định; chỉ loại trùng toàn dòng.

## 13. Lưu ý kỹ thuật

- Pipeline chạy Spark local với cấu hình mặc định `local[*]`, timezone `America/New_York`.
- Cấu hình có giới hạn `spark.sql.files.maxPartitionBytes`; giá trị cần được đánh giá với tài nguyên máy chạy thực tế.
- Bronze và Silver được ghi đè khi chạy lại.
- Zone lookup được dùng khi có cấu hình hoặc tệp lookup theo vị trí mặc định; khi không có, pipeline vẫn tạo bảng theo mã khu vực.
- Không có khóa chuyến ổn định nên chỉ loại bản ghi trùng khi cả 18 trường nguồn giống nhau.
- Giá trị bất thường được ghi cảnh báo; không tự động loại khỏi Silver.
- Các báo cáo mô tả dữ liệu trong sáu tháng đầu năm 2020, không so sánh với năm 2019.

## 14. Tiến độ và mốc thực hiện

| Giai đoạn | Nội dung | Trạng thái |
|---|---|---|
| **1 — Ingestion và cleaning** | Đọc sáu CSV, schema, Bronze, Silver và báo cáo chất lượng | Đã triển khai |
| **2 — Taxi features và processed output** | Feature engineering, zone lookup tùy chọn, Parquet phân vùng | Đã triển khai |
| **3 — Aggregation và export** | Bảy bảng Gold, CSV, báo cáo Markdown và biểu đồ | Đã triển khai |
| **4 — Benchmark** | Script benchmark Spark persistence và Spark/Pandas | Đã triển khai; kết quả cần đo trên máy mục tiêu |
| **5 — Chạy dữ liệu thật và demo** | Chạy đủ sáu file, lưu baseline môi trường và chuẩn bị trình bày | Việc cần hoàn tất theo môi trường nhóm |

## 15. Rủi ro, giả định và câu hỏi mở

### Rủi ro

| Rủi ro | Ảnh hưởng | Hướng xử lý |
|---|---|---|
| Dung lượng sáu tệp vượt tài nguyên máy local | Chạy chậm hoặc thiếu bộ nhớ/ổ đĩa | Chạy benchmark trên máy mục tiêu và điều chỉnh partition |
| Header hoặc schema thay đổi giữa các tệp | Lỗi đọc hoặc sai ánh xạ cột | Kiểm tra schema 18 trường và dùng `FAILFAST` |
| Không có khóa chuyến ổn định | Không thể phát hiện mọi bản ghi trùng nghiệp vụ | Chỉ loại trùng toàn dòng và ghi rõ giới hạn |
| Mã khu vực không ánh xạ được qua lookup | Báo cáo theo tên khu vực thiếu | Đếm mã không khớp và giữ chuyến |
| Giá trị âm có ý nghĩa nghiệp vụ | Loại nhầm dữ liệu điều chỉnh | Giữ bản ghi và ghi cảnh báo |
| Timestamp có khác biệt theo DST | Sai nhóm theo ngày hoặc giờ | Dùng timezone `America/New_York` nhất quán |

### Giả định

- Người dùng có đủ sáu tệp CSV từ 01/2020 đến 06/2020.
- Các tệp có header và tương thích schema taxi vàng 18 cột.
- Máy chạy có Python 3.10+, PySpark 3.5.x, Java 11 hoặc 17 và dung lượng phù hợp.
- Phân tích Gold là mô tả trong phạm vi dữ liệu đã chọn.

### Câu hỏi mở

1. Máy mục tiêu có bao nhiêu RAM và dung lượng ổ đĩa khả dụng để ghi nhận baseline?
2. Khi demo, nhóm có cung cấp lookup taxi zone để hiển thị tên khu vực hay chỉ dùng mã?

## 16. Phụ thuộc

- **Dữ liệu bắt buộc:** sáu CSV tháng 01–06/2020.
- **Dữ liệu tùy chọn:** bảng lookup taxi zone.
- **Nguồn:** New York Yellow Taxi Trip Data trên Kaggle; dữ liệu gốc từ NYC TLC.
- **Runtime:** Python 3.10+, PySpark 3.5.x, Java 11/17.
- **Cấu hình:** `configs/config.example.yaml` và cấu hình cục bộ `configs/config.yaml`.
- **Hạ tầng:** quyền đọc dữ liệu Raw, quyền ghi Bronze/Silver/Gold và reports.
- **Kiểm thử:** fixtures nhỏ dùng trong CI; chạy trên đủ sáu tệp là bước xác nhận với dữ liệu thật.

## 17. Phụ lục

### Thuật ngữ

- **Raw:** Tệp nguồn chưa qua xử lý.
- **Bronze:** Dữ liệu đã được đọc và lưu ở định dạng thuận tiện cho xử lý Spark.
- **Silver:** Dữ liệu sau làm sạch và kiểm tra chất lượng.
- **Gold:** Dữ liệu đã tổng hợp, phục vụ câu hỏi phân tích.
- **Grain:** Ý nghĩa của một dòng trong bảng, ví dụ một dòng cho mỗi tháng hoặc mỗi mã khu vực.
- **Exact duplicate:** Hai bản ghi giống nhau ở toàn bộ 18 trường.

### Tài liệu tham khảo

- [New York Yellow Taxi Trip Data — Kaggle](https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- `src/ingestion/schema.py`
- `src/pipeline/main.py`
- `docs/aggregations.md`
- `docs/benchmarking.md`
- `src/cleaning/taxi.py`
- `src/cleaning/quality.py`
