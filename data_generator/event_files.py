"""Write identical learning-event batches as JSON Lines and CSV."""

import csv
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory

LOCAL_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "local"
FIELDS = (
    "event_id", "student_id", "event_type", "event_time", "ingest_time",
    "video_id", "duration_sec", "quiz_score", "metadata",
)


def write_events(events: Iterable[Mapping[str, object]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output_dir) as staging:
        json_path = Path(staging) / "learning_events.json"
        csv_path = Path(staging) / "learning_events.csv"
        with (
            json_path.open("w", encoding="utf-8") as json_file,
            csv_path.open("w", encoding="utf-8", newline="") as csv_file,
        ):
            csv_writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
            csv_writer.writeheader()
            for event in events:
                json_file.write(json.dumps(dict(event)) + "\n")
                csv_writer.writerow({**event, "metadata": json.dumps(event["metadata"])})

        json_path.replace(output_dir / json_path.name)
        csv_path.replace(output_dir / csv_path.name)
