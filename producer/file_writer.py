import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from utils.logger import get_logger


logger = get_logger("file-writer")

BRONZE_DIRECTORY = Path("data/bronze")


def write_events(events: list[dict]) -> str:
    """
    Write one batch of events to a unique JSONL file.

    Returns the path of the created JSONL file.
    """

    BRONZE_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    current_time = datetime.now(timezone.utc)

    file_name = (
        f"booking_events_"
        f"{current_time.strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid4().hex}.jsonl"
    )

    file_path = BRONZE_DIRECTORY / file_name

    with file_path.open(
        mode="w",
        encoding="utf-8",
    ) as file:
        for event in events:
            json_line = json.dumps(event)
            file.write(json_line + "\n")

    logger.info(
        "Bronze batch written locally | records=%s | file=%s",
        len(events),
        file_path,
    )

    return str(file_path)