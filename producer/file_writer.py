import json
from pathlib import Path


BRONZE_FILE_PATH = Path("data/bronze/booking_events.jsonl")


def write_event(event: dict) -> None:
    BRONZE_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with BRONZE_FILE_PATH.open(mode="a", encoding="utf-8") as file:
        file.write(json.dumps(event) + "\n")


def write_events(events: list[dict]) -> None:
    BRONZE_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with BRONZE_FILE_PATH.open(mode="a", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")