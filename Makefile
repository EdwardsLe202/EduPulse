.PHONY: install generate-data generate-synthetic-data phase1-check spark-basics

install:
	uv sync --locked

generate-data:
	SPARK_LOCAL_IP=127.0.0.1 uv run --locked python data_generator/import_ednet.py

generate-synthetic-data:
	uv run --locked python data_generator/generate_history.py

phase1-check:
	uv run --locked python spark_basics/smoke_check.py

spark-basics:
	SPARK_LOCAL_IP=127.0.0.1 uv run --locked pyspark --master 'local[2]' \
		--conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=4
