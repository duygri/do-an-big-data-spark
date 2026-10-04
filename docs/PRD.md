# PRD: Pipeline nhập dữ liệu taxi vàng New York bằng PySpark

## 1. Thông tin tài liệu

| Trường | Giá trị |
|---|---|
| **Tên tài liệu** | Product Requirements Document — Pipeline nhập dữ liệu taxi vàng New York bằng PySpark |
| **Phiên bản** | 0.1 |
| **Tác giả** | Nhóm dự án (PM phụ trách: cần bổ sung) |
| **Trạng thái** | Bản dự thảo để rà soát |
| **Cập nhật lần cuối** | 02/10/2026 |

## 2. Tổng quan và vấn đề cần giải quyết

Dự án xây dựng pipeline batch để nhập và xử lý dữ liệu chuyến taxi vàng tại New York bằng Python và Apache Spark. Dữ liệu gồm 18 tệp CSV theo tháng, từ tháng 01/2019 đến tháng 06/2020.

Xử lý các tệp lớn bằng pandas hoặc Python thuần có thể vượt quá giới hạn bộ nhớ của một máy. Dự án sử dụng PySpark để thực hành đọc dữ liệu theo schema tường minh, lưu trữ dữ liệu dạng Parquet và thực hiện các bước làm sạch có thể kiểm tra lại.

Pipeline hiện có các bước đọc CSV, ghi lớp Bronze, làm sạch sang lớp Silver và xuất báo cáo chất lượng dạng JSON và Markdown. Lớp Gold và các phép tổng hợp để trả lời câu hỏi phân tích chưa được triển khai. Bảng tra cứu khu vực cũng chưa được đọc vào pipeline.

## 3. Mục tiêu và mục đích

### Mục tiêu học phần

- Thực hành nhập dữ liệu nhiều tệp và dung lượng lớn bằng Spark.
- Áp dụng schema tường minh để kiểm soát kiểu dữ liệu.
- Lưu dữ liệu trung gian bằng Parquet và nén Snappy.
- Thiết kế các bước làm sạch, kiểm tra chất lượng và lưu vết số dòng.
- Tạo nền tảng dữ liệu có thể dùng cho phân tích theo thời gian, khu vực và đặc điểm chuyến đi.

### Mục tiêu sản phẩm

1. Duy trì pipeline Raw → Bronze → Silver có thể chạy lại bằng cấu hình.
2. Đảm bảo số dòng và schema được xác minh sau khi ghi Bronze và Silver.
3. Tạo báo cáo chất lượng rõ ràng cho mỗi lần chạy thành công.
4. Phát triển lớp Gold để hỗ trợ các câu hỏi phân tích đã nêu.
5. Ghi rõ giới hạn dữ liệu và quy tắc lọc để người đọc không diễn giải quá mức kết quả.

## 4. Chỉ số thành công / KPI

| Chỉ số | Mục tiêu | Trạng thái |
|---|---|---|
| Phạm vi dữ liệu | Chạy đầy đủ 18 tệp tháng; cho phép chạy thử một tệp | Cần bổ sung kiểm tra tính đầy đủ của bộ tệp khi chạy toàn bộ |
| Tính toàn vẹn schema | Đọc dữ liệu theo schema 18 cột đã khai báo | Đã triển khai |
| Xác minh Bronze | Số dòng và schema sau khi đọc lại Parquet khớp với đầu vào | Đã triển khai |
| Xác minh Silver | Số dòng và schema sau khi đọc lại Parquet khớp với dữ liệu đã làm sạch | Đã triển khai |
| Báo cáo chất lượng | Mỗi lần chạy thành công tạo đủ báo cáo JSON và Markdown | Đã triển khai |
| Số dòng bị loại | Xác định được số dòng bị loại do thiếu trường bắt buộc và do trùng chính xác | Đã triển khai |
| Kết quả Gold | Các bảng tổng hợp hỗ trợ câu hỏi phân tích trong mục 7 và 8 | Chưa triển khai |
| Thời gian chạy | Ghi nhận thời gian chạy với bộ dữ liệu đầy đủ; đặt ngưỡng sau khi đo trên môi trường mục tiêu | Chưa có baseline |

## 5. Người dùng và bên liên quan

| Vai trò | Nhu cầu |
|---|---|
| **Sinh viên / nhóm phát triển** | Chạy pipeline, kiểm tra dữ liệu qua từng lớp và phát triển các bước Gold |
| **Giảng viên** | Đánh giá cách thiết kế pipeline, xử lý dữ liệu lớn và kiểm soát chất lượng |
| **Người phân tích dữ liệu** | Truy vấn số chuyến, khu vực, quãng đường, cước phí và khác biệt giữa các giai đoạn |

## 6. Phạm vi

### Trong phạm vi

- Nhập các tệp CSV dữ liệu taxi vàng theo tháng.
- Áp dụng schema tường minh gồm 18 cột.
- Ghi dữ liệu Bronze và Silver dưới dạng Parquet nén Snappy.
- Thực hiện các quy tắc làm sạch hiện có và xuất báo cáo chất lượng.
- Phát triển các phép tổng hợp Gold theo thời gian, khu vực và đặc điểm chuyến đi.
- So sánh các chỉ số giữa các giai đoạn 2019 và sáu tháng đầu năm 2020.
- Cho phép chạy toàn bộ dữ liệu hoặc một tệp để thử nghiệm.

### Ngoài phạm vi

- Xử lý dữ liệu streaming hoặc dữ liệu thời gian thực.
- Tích hợp nguồn dữ liệu ngoài bộ NYC Yellow Taxi Trip Data đã nêu.
- Xây dựng dashboard hoặc ứng dụng phục vụ người dùng cuối.
- Dự báo, machine learning hoặc kết luận quan hệ nhân quả.
- Loại bỏ bản ghi chỉ vì có giá trị bất thường; các cảnh báo hiện được ghi nhận nhưng bản ghi vẫn được giữ lại.
- Cung cấp hạ tầng cụm Spark hoặc dịch vụ triển khai production.

## 7. User stories và trường hợp sử dụng

### User stories

- **Là sinh viên**, tôi muốn chạy pipeline trên toàn bộ các tệp tháng để xử lý dữ liệu bằng Spark.
- **Là sinh viên**, tôi muốn chạy thử một tệp tháng để kiểm tra cấu hình trước khi xử lý toàn bộ dữ liệu.
- **Là người kiểm tra dữ liệu**, tôi muốn biết số dòng bị loại ở từng bước và các cảnh báo chất lượng.
- **Là người phân tích**, tôi muốn tổng hợp số chuyến theo thời gian và khu vực để tìm các xu hướng chính.
- **Là giảng viên**, tôi muốn xem rõ schema, quy tắc làm sạch và kết quả đối soát để đánh giá tính đúng đắn của pipeline.

### Trường hợp sử dụng chính

1. Người dùng tải và giải nén dữ liệu, đặt các tệp CSV vào thư mục Raw.
2. Người dùng sao chép cấu hình mẫu và điều chỉnh đường dẫn nếu cần.
3. Pipeline kiểm tra đầu vào, đọc dữ liệu theo schema và ghi Bronze.
4. Pipeline đọc lại Bronze, làm sạch dữ liệu, ghi Silver và tạo báo cáo chất lượng.
5. Khi lớp Gold hoàn tất, người dùng chạy các phép tổng hợp và xem các bảng kết quả để trả lời câu hỏi phân tích.

## 8. Yêu cầu chức năng

Các trạng thái dưới đây phân biệt **hiện trạng** với **yêu cầu cho phiên bản mục tiêu**.

### Must-have

#### FR-01 — Đọc CSV theo schema

**Trạng thái:** Đã triển khai.

Pipeline phải đọc CSV có header theo schema taxi vàng 18 cột.

**Tiêu chí nghiệm thu**

- Đầu vào có thể là một tệp hoặc một nhóm tệp khớp với đường dẫn glob trong cấu hình.
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
- Có thể trỏ `input.path` tới một tệp CSV để chạy thử.
- Thiếu file cấu hình hoặc cấu hình sai phải khiến chương trình kết thúc với mã lỗi và ghi thông tin lỗi vào log.
- Các đường dẫn tương đối được tính từ thư mục hiện hành khi chạy.

#### FR-06 — Lưu số liệu và đối soát cho Gold

**Trạng thái:** Chưa triển khai; yêu cầu cho phiên bản mục tiêu.

Mỗi bảng Gold phải có grain rõ ràng và được lưu ở định dạng phù hợp để tiếp tục truy vấn bằng Spark.

**Tiêu chí nghiệm thu**

- Mỗi bảng mô tả rõ đơn vị của một dòng, các khóa nhóm và chỉ số được tính.
- Tổng số chuyến trong bảng tổng hợp thời gian đối soát được với tập dữ liệu Silver áp dụng cùng điều kiện lọc.
- Quy tắc loại hoặc giữ bản ghi bất thường được ghi cùng tài liệu Gold.
- Chạy lại cùng đầu vào và cùng cấu hình tạo kết quả logic nhất quán.

#### FR-07 — Tổng hợp theo thời gian

**Trạng thái:** Chưa triển khai; yêu cầu cho phiên bản mục tiêu.

Gold phải hỗ trợ xem số chuyến theo tháng, ngày trong tuần và giờ trong ngày.

**Tiêu chí nghiệm thu**

- Có kết quả tổng hợp số chuyến theo tháng.
- Có kết quả tổng hợp số chuyến theo ngày trong tuần.
- Có kết quả tổng hợp số chuyến theo giờ trong ngày.
- Tên cột và grain của từng bảng được mô tả trong tài liệu.
- Các giá trị thời gian được tạo từ timestamp đón; múi giờ và cách xử lý timestamp được ghi rõ.

#### FR-08 — Tổng hợp theo khu vực đón/trả

**Trạng thái:** Chưa triển khai; yêu cầu cho phiên bản mục tiêu.

Gold phải hỗ trợ xếp hạng mã khu vực đón và trả theo số chuyến.

**Tiêu chí nghiệm thu**

- Có bảng riêng cho số chuyến theo `PULocationID` và `DOLocationID`.
- Mỗi kết quả có mã khu vực và số chuyến.
- Quy tắc xử lý mã khu vực không hợp lệ được xác định trước khi tổng hợp.
- Nếu lookup chưa được nối, kết quả ghi rõ đang hiển thị mã thay vì tên khu vực.

#### FR-09 — Tổng hợp đặc điểm chuyến đi và thanh toán

**Trạng thái:** Chưa triển khai; yêu cầu cho phiên bản mục tiêu.

Gold phải hỗ trợ mô tả quãng đường, tiền cước, tiền tip và cơ cấu phương thức thanh toán.

**Tiêu chí nghiệm thu**

- Với `trip_distance`, `fare_amount` và `tip_amount`, kết quả có số quan sát hợp lệ và các thống kê mô tả được thống nhất.
- Với `payment_type`, kết quả có số chuyến theo mã thanh toán.
- Nếu hiển thị tỷ lệ phần trăm, mẫu số và cách xử lý giá trị null được nêu rõ.
- Nếu dùng histogram hoặc nhóm khoảng, biên nhóm và đơn vị được ghi lại; không tự diễn giải nhóm là phân loại chính thức của nguồn.

#### FR-10 — So sánh giai đoạn 2019 và nửa đầu 2020

**Trạng thái:** Chưa triển khai; yêu cầu cho phiên bản mục tiêu.

Gold phải hỗ trợ so sánh các chỉ số được chọn giữa năm 2019 và sáu tháng đầu năm 2020.

**Tiêu chí nghiệm thu**

- Kết quả ghi rõ ngày bắt đầu, ngày kết thúc và số tháng được đưa vào từng kỳ.
- Khi so sánh xu hướng theo tháng, hỗ trợ so sánh cùng các tháng 01–06 của hai năm.
- Nếu so sánh cả năm 2019 với sáu tháng đầu 2020, báo cáo ghi rõ hai kỳ có thời lượng khác nhau.
- Kết quả chỉ được trình bày là mô tả dữ liệu; không kết luận nguyên nhân từ chênh lệch nếu chưa có phương pháp phân tích phù hợp.

### Should-have

#### FR-11 — Nối lookup khu vực

**Trạng thái:** Chưa triển khai.

Pipeline nên đọc bảng tra cứu khu vực để hiển thị tên khu vực thay cho mã.

**Tiêu chí nghiệm thu**

- Lookup được nối riêng cho khu vực đón và khu vực trả.
- Bản ghi không tìm thấy mã khu vực được đếm và báo cáo; không âm thầm làm mất chuyến.
- Báo cáo phân biệt mã khu vực nguồn và tên khu vực sau khi nối.
- Phiên bản và nguồn của lookup được ghi nhận.

#### FR-12 — Kiểm tra tính đầy đủ của bộ dữ liệu

**Trạng thái:** Cần bổ sung.

Khi người dùng chọn chế độ chạy toàn bộ dữ liệu, pipeline nên phát hiện thiếu tệp tháng trong phạm vi 01/2019–06/2020.

**Tiêu chí nghiệm thu**

- Chế độ chạy toàn bộ có danh sách kỳ vọng gồm 18 tháng.
- Thiếu tháng được nêu rõ trong log hoặc báo cáo.
- Chạy thử một tệp vẫn được phép mà không yêu cầu đủ 18 tháng.

### Nice-to-have

Chưa xác định yêu cầu chức năng bổ sung ngoài phạm vi trên. Các tính năng Nice-to-have chỉ được đưa vào backlog sau khi nhóm thống nhất nhu cầu và không làm ảnh hưởng các tiêu chí Must-have.

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

- Bộ dữ liệu: New York Yellow Taxi Trip Data trên Kaggle.
- Nguồn gốc dữ liệu: NYC Taxi & Limousine Commission (TLC).
- Phạm vi: 18 tệp theo tháng, từ 01/2019 đến 06/2020.
- Định dạng đầu vào hiện tại: CSV có header.
- Bảng tra cứu khu vực có trong nguồn dữ liệu nhưng chưa được pipeline hiện tại đọc.

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

```text
18 tệp CSV
    │
    ▼
data/raw/nyc_taxi/
    │  PySpark đọc theo schema 18 cột
    ▼
data/bronze/raw/
    │  Parquet Snappy; đọc lại và đối soát schema/số dòng
    ▼
data/silver/cleaned/
    │  Bỏ null bắt buộc, loại trùng chính xác,
    │  chuẩn hóa cờ và kiểm tra chất lượng
    ├── reports/generated/taxi_quality.json
    └── reports/generated/taxi_quality.md
```

### Trạng thái mục tiêu

```text
Raw CSV
  → Bronze Parquet
  → Silver Parquet + báo cáo chất lượng
  → Gold Parquet
      ├── Tổng hợp thời gian
      ├── Tổng hợp khu vực đón/trả
      ├── Thống kê chuyến đi và thanh toán
      └── So sánh các giai đoạn
```

Lớp Gold và các phép tổng hợp là phần phát triển tiếp theo, chưa có trong pipeline hiện tại.

## 12. Quy tắc chất lượng và xác thực dữ liệu

### 12.1 Kiểm tra đầu vào

- Bắt buộc có đường dẫn đầu vào khớp ít nhất một tệp.
- Định dạng đọc phải thuộc loại reader hỗ trợ.
- CSV phải có header theo cấu hình.
- Schema taxi vàng hiện tại gồm 18 trường theo định nghĩa trong mã.
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

- Pipeline hiện chạy Spark local với cấu hình mặc định `local[*]`.
- Cấu hình có giới hạn `spark.sql.files.maxPartitionBytes`; giá trị cần được đánh giá với tài nguyên máy chạy thực tế.
- Bronze và Silver đang được ghi theo chế độ overwrite; chạy lại sẽ thay thế dữ liệu hiện có tại các đường dẫn này.
- Pipeline chỉ xử lý CSV chuyến đi trong cấu hình mặc định; bảng tra cứu và dữ liệu vùng chưa được nạp.
- Không có khóa chuyến ổn định nên không thể khẳng định hai bản ghi giống một phần là cùng một chuyến.
- Thứ tự bản ghi trong Parquet không được xem là thứ tự nghiệp vụ.
- Cần thống nhất múi giờ trước khi tạo các nhóm theo giờ/ngày.
- So sánh cả năm 2019 với sáu tháng đầu năm 2020 có độ dài kỳ khác nhau; phân tích xu hướng nên ưu tiên so sánh cùng kỳ tháng 01–06.

## 14. Tiến độ và mốc thực hiện đề xuất

| Giai đoạn | Nội dung | Kết quả |
|---|---|---|
| **0 — Đã hoàn thành** | Đọc CSV, schema 18 cột, ghi Bronze, làm sạch Silver và báo cáo chất lượng | Pipeline Raw → Bronze → Silver hiện có |
| **1 — Hoàn thiện tiêu chí Gold** | Chốt timezone, quy tắc mã khu vực, kỳ so sánh và grain các bảng | Đặc tả Gold được nhóm duyệt |
| **2 — Gold theo thời gian và khu vực** | Tổng hợp tháng/ngày/giờ và mã khu vực đón/trả | Các bảng Gold đầu tiên |
| **3 — Gold theo đặc điểm chuyến đi** | Thống kê quãng đường, cước, tip và phương thức thanh toán | Các bảng phân bố và cơ cấu thanh toán |
| **4 — Kiểm thử và đối soát** | Kiểm tra trên mẫu nhỏ, đối soát tổng số, chạy toàn bộ dữ liệu | Báo cáo nghiệm thu pipeline |
| **5 — Demo và hoàn thiện tài liệu** | Tổng hợp kết quả, giới hạn và hướng phát triển | Demo học phần và tài liệu dự án |

Thời lượng từng giai đoạn cần được ước lượng sau khi nhóm xác nhận lịch học, tài nguyên máy và trạng thái dữ liệu đầu vào.

## 15. Rủi ro, giả định và câu hỏi mở

### Rủi ro

| Rủi ro | Ảnh hưởng | Hướng xử lý |
|---|---|---|
| Dung lượng dữ liệu vượt tài nguyên máy local | Chạy chậm hoặc thiếu bộ nhớ/ổ đĩa | Chạy thử một tháng, ghi nhận baseline và điều chỉnh partition |
| Schema thay đổi giữa các tệp hoặc phiên bản dữ liệu | Lỗi đọc hoặc sai ánh xạ cột | Kiểm tra header và schema trước khi chạy toàn bộ |
| Không có khóa chuyến ổn định | Không thể phát hiện mọi bản ghi trùng nghiệp vụ | Giữ quy tắc loại trùng toàn dòng và ghi rõ giới hạn |
| Mã khu vực không ánh xạ được qua lookup | Báo cáo theo tên khu vực thiếu hoặc sai | Đếm mã không khớp và bảo toàn bản ghi |
| Giá trị âm có ý nghĩa nghiệp vụ | Loại nhầm dữ liệu điều chỉnh | Tiếp tục giữ cảnh báo trong Silver; chỉ lọc khi có quy tắc được duyệt |
| So sánh hai kỳ không tương đương | Diễn giải sai khác biệt | So sánh cùng kỳ tháng 01–06 và ghi rõ phạm vi |
| Timestamp/múi giờ không được thống nhất | Sai nhóm theo ngày hoặc giờ | Chốt cách xử lý timestamp trước khi triển khai Gold |

### Giả định

- Các tệp đầu vào đúng là bộ NYC Yellow Taxi Trip Data theo phạm vi tháng 01/2019–06/2020.
- Các tệp CSV có header và tương thích với schema 18 cột trong mã hiện tại.
- Máy chạy có Python 3.10+, PySpark 3.5.x, Java 11 hoặc 17 và đủ dung lượng để lưu Parquet trung gian.
- Phân tích ở lớp Gold là phân tích mô tả; không nhằm chứng minh nguyên nhân của các thay đổi.

### Câu hỏi mở

1. Máy mục tiêu có bao nhiêu RAM và dung lượng ổ đĩa khả dụng?
2. Có cần hiển thị tên khu vực trong báo cáo Gold hay chỉ cần mã `PULocationID`/`DOLocationID`?
3. Các mã khu vực nhỏ hơn hoặc bằng 0 có bị loại khỏi thống kê Gold hay được gom thành nhóm riêng?
4. Kỳ so sánh chính sẽ là 01–06/2019 với 01–06/2020, hay cả năm 2019 với nửa đầu 2020?
5. Cần định nghĩa histogram cho quãng đường và tiền cước theo các khoảng cố định hay chỉ cần thống kê mô tả?
6. Múi giờ nào được dùng khi nhóm chuyến theo giờ và ngày?

## 16. Phụ thuộc

- **Dữ liệu:** 18 tệp CSV tháng và bảng lookup khu vực nếu triển khai tổng hợp theo tên.
- **Nguồn dữ liệu:** Bộ New York Yellow Taxi Trip Data trên Kaggle; dữ liệu gốc từ NYC TLC.
- **Runtime:** Python 3.10+, PySpark 3.5.x, Java 11/17.
- **Cấu hình:** `configs/config.example.yaml` và cấu hình cục bộ `configs/config.yaml`.
- **Hạ tầng:** Quyền đọc dữ liệu Raw, quyền ghi các thư mục Bronze/Silver/Gold và reports.
- **Kiểm thử:** Dữ liệu mẫu đủ nhỏ để kiểm tra logic mà không cần xử lý toàn bộ bộ dữ liệu.

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
- `src/cleaning/taxi.py`
- `src/cleaning/quality.py`
