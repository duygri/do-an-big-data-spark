# Thiết kế: Gold aggregations, báo cáo và benchmark taxi

## Mục tiêu

Hoàn thành các issue #12–#15 cho bộ dữ liệu NYC Yellow Taxi: tạo các bảng Gold trả lời câu hỏi phân tích trong PRD, đo một tối ưu Spark có thể tái lập, xuất kết quả dùng được trong báo cáo môn học, và so sánh cùng một workload trên Spark và Pandas.

Các issue dùng một số thuật ngữ mẫu không khớp miền dữ liệu (ví dụ “top products”). Thiết kế này dùng chỉ số taxi trong PRD; “top products/categories” được ánh xạ thành thứ hạng khu vực đón/trả và cơ cấu phương thức thanh toán.

## Ranh giới và luồng dữ liệu

Giữ ingestion, cleaning và feature engineering hiện có. Sau khi pipeline tạo feature và tùy chọn nối zone lookup, tầng aggregation tạo các DataFrame Spark riêng cho từng grain. Pipeline ghi các bảng tổng hợp vào Gold dưới dạng Parquet, đồng thời xuất CSV và báo cáo nhỏ vào `reports/generated/`. Dữ liệu chuyến chỉ được persist trong thời gian nhiều aggregation và output cùng dùng nó; cache được giải phóng trong `finally`. Nếu zone join đã sở hữu cache cho cùng dữ liệu, pipeline dùng cache đó thay vì tạo một bản cache thứ hai.

Các hàm aggregation chỉ tạo DataFrame, không ghi file hay thu toàn bộ dữ liệu về driver. Writer phụ trách Parquet/CSV và xác minh. Phần report chỉ thu các bảng tổng hợp nhỏ về driver để dựng Markdown và biểu đồ; không đưa chuyến thô vào Pandas.

## Hợp đồng các bảng Gold

| Bảng | Grain | Nội dung chính |
| --- | --- | --- |
| `trips_by_month` | Một dòng mỗi năm/tháng đón | Số chuyến, tổng/giá trị trung bình của fare và tip |
| `trips_by_weekday` | Một dòng mỗi thứ trong tuần | Số chuyến; thứ theo quy ước Spark, Chủ nhật = 1 |
| `trips_by_hour` | Một dòng mỗi giờ đón | Số chuyến, giờ theo timezone Spark `America/New_York` |
| `pickup_zones`, `dropoff_zones` | Một dòng mỗi mã khu vực | Mã, tên/borough nếu lookup có sẵn, số chuyến; sắp hạng xác định bằng số chuyến giảm dần rồi mã tăng dần |
| `trip_metric_stats` | Một dòng mỗi metric | Số quan sát khác null, min, mean, median xấp xỉ (accuracy 10.000), max và độ lệch chuẩn cho quãng đường, fare và tip; giá trị âm được giữ theo Silver |
| `payment_mix` | Một dòng mỗi mã payment | Số chuyến và tỷ trọng trên toàn bộ chuyến; null được tính vào nhóm chưa biết và mẫu số |
| `same_month_comparison` | Một dòng mỗi tháng 1–6 có dữ liệu ở cả hai năm | Số chuyến, tổng fare, fare trung bình, tổng tip, tip trung bình và quãng đường trung bình của từng năm; chênh lệch tuyệt đối và phần trăm, phần trăm để null nếu mẫu số 2019 bằng 0 |

Timestamp được nhóm theo timezone phiên Spark hiện cấu hình là `America/New_York`. Bảng zone giữ một nhóm riêng cho mã null hoặc `<= 0`, thay vì loại chuyến; khi không có lookup, bảng chỉ báo mã. Bảng so sánh chỉ so các tháng tương ứng 1–6 và được diễn giải mô tả, không suy luận nguyên nhân.

Tổng số chuyến ở các grain thời gian, zone và payment phải đối soát được với đầu vào theo quy tắc null tương ứng. Thứ tự Parquet không mang ý nghĩa nghiệp vụ.

## Báo cáo và benchmark

Parquet là nguồn Gold để truy vấn tiếp. Mỗi bảng cũng có CSV có header. Báo cáo Markdown ghi phạm vi ngày, số tháng, grain, định nghĩa metric, điều kiện giữ dữ liệu và tóm tắt kết quả. Biểu đồ nhỏ dùng các kết quả tổng hợp: xu hướng chuyến theo tháng, top khu vực đón/trả và payment mix. Các bảng toàn bộ vẫn được giữ trong Parquet/CSV; top-N chỉ giới hạn phần trình bày.

Hai lệnh benchmark chạy tách khỏi pipeline thường ngày:

1. **Spark optimization (#13):** chạy cùng tập aggregation nhiều lần ở chế độ baseline (không persist input dùng chung) và tối ưu (persist input một lần), xác minh kết quả tương đương và ghi thời gian, số dòng, tốc độ, cấu hình/runtime cùng physical-plan hoặc event-log evidence. Thời gian materialize cache được tính vào chế độ tối ưu.
2. **Spark vs Pandas (#15):** chạy cùng workload gồm tổng hợp tháng, thống kê fare/tip/distance, khu vực và payment trên cùng các tệp CSV; ghi riêng thời gian khởi tạo Spark, thời gian đọc và xử lý, throughput, kích thước dữ liệu, phiên bản thư viện và mức dùng bộ nhớ tối đa quan sát được. Có thể truyền nhiều bộ tệp cỡ khoảng 100 MB, 1 GB, 5 GB hoặc các mẫu lớn nhất mà máy chạy được; không tự chạy các benchmark nặng trong pipeline.

Mỗi benchmark ghi kết quả CSV/JSON kèm thông tin môi trường và lệnh chạy. Không commit dữ liệu đầu vào hoặc kết quả benchmark theo dung lượng lớn. Benchmark thực tế phụ thuộc dữ liệu và phần cứng của người chạy; CI chỉ kiểm tra tính đúng trên bộ dữ liệu nhỏ.

## Xử lý lỗi và kiểm thử

- Lỗi schema/thiếu cột bắt buộc phải báo rõ cột thiếu trước khi tạo bảng.
- Writer kiểm tra schema và số dòng khi đọc lại Parquet; lỗi ghi hoặc sai đối soát làm pipeline thất bại.
- Cache được giải phóng kể cả khi một bảng hoặc biểu đồ lỗi.
- Kiểm thử Spark local dùng các chuyến nhỏ có kết quả tính tay: biên thời gian, giá trị null/âm, mã zone chưa biết, payment null, tháng thiếu và chênh lệch giữa hai kỳ.
- Kiểm thử writer xác nhận tên, schema, số dòng, CSV và report. Kiểm thử benchmark xác nhận kết quả Spark/Pandas bằng nhau trên fixture nhỏ, không đặt ngưỡng thời gian phụ thuộc máy.

## Ngoài phạm vi

Không xây dựng dashboard, không dùng Pandas cho bảng chuyến lớn, không tự lọc chuyến bất thường ngoài quy tắc đã nêu, và không đưa benchmark nặng vào lần chạy pipeline thông thường. Không dùng các chênh lệch mô tả để kết luận quan hệ nhân quả.
