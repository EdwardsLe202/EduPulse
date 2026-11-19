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

Hoàn thành các TODO theo thứ tự dưới đây. Các lệnh chạy từ thư mục gốc.

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

Để quan sát lazy evaluation, dùng `spark.range()` trước vì bước suy luận schema khi đọc file có thể tự chạy job. Khi làm bài DataFrame và SQL, kiểm tra kiểu cột `event_time` ở cả hai định dạng trước khi nhóm theo ngày.

## Hoàn thành Phase 1

- [ ] Hoàn thành ba script và đối chiếu bảng đếm theo ngày.
- [ ] Lưu ghi chú và một ảnh Spark UI vào `spark_basics/` từ kết quả bạn quan sát.
- [ ] Tự giải thích trong hai phút driver làm gì, partition liên quan thế nào đến task, action tạo job ra sao và vì sao shuffle có thể chia job thành nhiều stage.
