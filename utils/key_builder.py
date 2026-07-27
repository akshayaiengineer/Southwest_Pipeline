from datetime import datetime, timezone
from uuid import uuid4

def create_bronze_s3_key() -> str:
    """
    Create a unique, time-partitioned S3 object key
    for one Bronze booking-event batch.
    """

    current_time = datetime.now(timezone.utc)

    unique_file_id = uuid4().hex

    return (
        f"bronze/"
        f"year={current_time.year}/"
        f"month={current_time.month:02d}/"
        f"day={current_time.day:02d}/"
        f"hour={current_time.hour:02d}/"
        f"booking_events_{unique_file_id}.jsonl.gz"
    )