# Phase 1 · Spark Foundations

Mục tiêu tuần đầu là đọc được dữ liệu bằng PySpark, đối chiếu kết quả với Spark SQL và Pandas, rồi dùng Spark UI để giải thích một job và một shuffle.

## Khởi động

Chạy từ thư mục gốc EduPulse. Môi trường dùng `uv`, Python 3.11, Java 17 và PySpark 4.0.3. `uv` tạo môi trường ảo `.venv` từ `uv.lock`.

```bash
java -version
make install
make generate-data
make phase1-check
```

Nếu macOS chọn sai phiên bản Java, chạy `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"` trong terminal đang làm bài.

`make generate-data` nhập **10.000 learning events thật** từ một shard EdNet KT3. Lần đầu tải khoảng 98 MB từ Hugging Face. Các lần sau dùng lại bản local sau khi kiểm tra SHA-256. Cùng một mẫu được ghi vào `data/local/learning_events.json` (mỗi dòng một JSON object) và `data/local/learning_events.csv`. `make phase1-check` kiểm tra Spark và Pandas đọc được cùng số dòng. [EdNet KT3 trên Hugging Face](https://huggingface.co/datasets/mgor/EDNet/viewer/kt3/train)

Mẫu bao phủ bảy ngày theo timestamp công bố, từ 08–14/04/2019 UTC. EdNet đã dịch timestamp vì lý do bảo mật. Sự kiện gồm thao tác mở bài, trả lời, nộp bài, xem và dừng video, mở và đóng phần giải thích. `quiz_score` ghi đúng/sai cho từng lần trả lời có đáp án. `ingest_time` và `duration_sec` để trống vì KT3 không ghi hai giá trị này. `video_stopped` ghi thao tác dừng xem. [Định nghĩa sự kiện và giấy phép CC BY-NC 4.0 của EdNet](https://github.com/riiid/ednet#kt3)

Mẫu này dùng cho bài tập khám phá dữ liệu. Khi thực hành churn, lấy một khoảng quan sát dài hơn với lịch sử đầy đủ của các học viên được chọn. Để tạo fixture mô phỏng, chạy `make generate-synthetic-data`. Fixture được ghi vào `data/local/synthetic/`.

Mở PySpark shell bằng `make spark-basics` để thử lệnh ngắn. Spark chạy `local[2]` với hai luồng thực thi cục bộ. Trong shell, xem địa chỉ Spark UI bằng `spark.sparkContext.uiWebUrl`, thường là `http://localhost:4040`. Giữ shell hoặc script đang chạy khi xem UI. [Tài liệu Spark UI](https://spark.apache.org/docs/4.0.3/monitoring.html)

## Bài tập

Bài 01 và 02 có lời giải để bạn chạy và đọc code. Bài 03 dừng ở từng bước để bạn tự quan sát Spark UI. Các lệnh chạy từ thư mục gốc.

| Bài | Việc cần làm | Cách tự kiểm tra |
|---|---|---|
| [01 · DataFrame](01_dataframe.py) | Đọc JSON và CSV. So sánh schema. Dùng `select`, `filter`, `withColumn`, `groupBy` để khám phá sự kiện và đếm theo ngày UTC. | Bảng đếm toàn bộ sự kiện có 7 ngày và tổng bằng 10.000 với dữ liệu mặc định. |
| [02 · Spark SQL và Pandas](02_sql.py) | Tạo temporary view rồi tính lại bảng đếm theo ngày bằng Spark SQL và Pandas. | Ba cách cho cùng số dòng và cùng số đếm theo ngày khi sắp xếp cùng thứ tự. |
| [03 · Lazy evaluation](03_lazy_evaluation.py) | Bắt đầu từ `spark.range()`, thêm transformation, quan sát tab Jobs trước và sau action. Chạy một phép nhóm cần shuffle rồi xem `explain()`, SQL và Stages. | Chụp Spark UI và chỉ ra job xuất hiện khi nào, các stage được chia ra ở đâu và dấu hiệu nào cho thấy dữ liệu bị shuffle. |

```bash
uv run --locked python spark_basics/01_dataframe.py
uv run --locked python spark_basics/02_sql.py
uv run --locked python spark_basics/03_lazy_evaluation.py
```

Với mẫu EdNet hiện tại, Spark suy luận `event_time` là `string` từ JSON và `timestamp` từ CSV. Cột `metadata` có kiểu `struct` trong JSON và `string` trong CSV. Hai bài đầu chuẩn hóa `event_time` về ngày UTC trước khi đếm.

### Quan sát Spark UI trong bài 03

Script in URL Spark UI khi khởi động. Mở URL đó trong trình duyệt và dùng cùng tab suốt bài. Nhấn Enter trong terminal để đi tiếp sau khi đã quan sát.

| Điểm dừng | Quan sát |
|---|---|
| Sau `filter()` và `explain()` | Tab **Jobs** đang trống. `filter()` thêm phép lọc vào kế hoạch và `explain()` in kế hoạch. `spark.range()` tạo đầu vào đơn giản để tránh các job do suy luận schema khi đọc file. |
| Sau `evens.count()` | Terminal in **50**. Tab **Jobs** có job của `Count even IDs`. Action `count()` yêu cầu Spark tính kết quả. Phép lọc xử lý từng partition, còn tổng đếm toàn cục có thể gom kết quả từng partition về một partition. |
| Sau khi chạy grouped count | Terminal in bốn bucket, mỗi bucket **25**. Trong tab **SQL**, mở truy vấn mới nhất và tìm `Exchange` với `hashpartitioning` theo `bucket`. Trong **Stages**, tìm số liệu **Shuffle Write** và **Shuffle Read** để thấy dữ liệu được chuyển giữa các stage. |

Mỗi task xử lý một partition trong một stage. Ở phép nhóm, Spark tính số đếm cục bộ rồi chuyển các kết quả có cùng `bucket` về cùng partition để gộp. `local[2]` cho phép chạy tối đa hai task đồng thời trong bài. Adaptive Query Execution có thể gộp các partition nhỏ sau shuffle, vì vậy số task sau shuffle có thể ít hơn bốn. [Tài liệu Spark UI](https://spark.apache.org/docs/4.0.3/monitoring.html), [Tài liệu Adaptive Query Execution](https://spark.apache.org/docs/4.0.3/sql-performance-tuning.html#adaptive-query-execution)

## Hoàn thành Phase 1

- [x] Hoàn thành ba script và đối chiếu bảng đếm theo ngày.
- [x] Lưu [ghi chú kết quả](notes.md) và [ảnh Spark UI](spark_ui_grouped_count.png) từ lần chạy bài 03.
- [ ] Tự giải thích trong hai phút driver làm gì, partition liên quan thế nào đến task, action tạo job ra sao và vì sao shuffle có thể chia job thành nhiều stage.
