"""Generate repeatable learning events for the local Spark exercises."""

import argparse
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

from event_files import LOCAL_DATA_DIR, write_events

DEFAULT_OUTPUT = LOCAL_DATA_DIR / "synthetic"
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
EVENT_TYPES = ("video_started", "video_completed", "quiz_submitted", "session_ended")


def generate_events(count: int, seed: int) -> Iterator[dict[str, object]]:
    rng = Random(seed)

    for index in range(1, count + 1):
        event_type = rng.choice(EVENT_TYPES)
        event_time = START + timedelta(seconds=rng.randrange(7 * 24 * 60 * 60))
        ingest_time = event_time + timedelta(seconds=rng.randint(0, 90))
        metadata = {"device": rng.choice(("web", "mobile"))}
        yield {
            "event_id": f"evt_{index:05d}",
            "student_id": f"stu_{rng.randint(1, 250):03d}",
            "event_type": event_type,
            "event_time": event_time.isoformat(),
            "ingest_time": ingest_time.isoformat(),
            "video_id": f"vid_{rng.randint(1, 50):02d}" if event_type.startswith("video_") else None,
            "duration_sec": rng.randint(60, 1800) if event_type == "session_ended" else None,
            "quiz_score": rng.randint(0, 10) / 10 if event_type == "quiz_submitted" else None,
            "metadata": metadata,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.events < 1:
        parser.error("--events must be greater than zero")

    write_events(generate_events(args.events, args.seed), args.output_dir)
    print(f"Generated {args.events:,} events in JSON and CSV at {args.output_dir}")


if __name__ == "__main__":
    main()
