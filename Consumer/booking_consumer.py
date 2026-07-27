import json
import os
import sys

from confluent_kafka import Consumer, KafkaError, TopicPartition
# Add the project root to Python's import path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)


from producer.file_writer import write_events
from utils.compression import compress_file
from utils.logger import get_logger
from utils.key_builder import create_bronze_s3_key
from utils.s3_uploader import upload_file_to_s3


logger = get_logger("booking-consumer")


KAFKA_TOPIC = "southwest-booking-events"
BATCH_SIZE = 10

S3_BUCKET_NAME = "data-pipeline-southwest"
AWS_REGION = "us-east-1"


KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "southwest-bronze-consumer-group-v2",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}


def commit_batch_offsets(
    consumer: Consumer,
    offsets_by_partition: dict[int, int],
) -> None:
    """
    Commit the next offset to read for each partition
    included in the successfully uploaded batch.
    """

    offsets = [
        TopicPartition(
            topic=KAFKA_TOPIC,
            partition=partition,
            offset=next_offset,
        )
        for partition, next_offset in offsets_by_partition.items()
    ]

    consumer.commit(
        offsets=offsets,
        asynchronous=False,
    )


def process_batch(
    consumer: Consumer,
    event_batch: list[dict],
    batch_offsets: dict[int, int],
) -> None:
    """
    Process one Kafka batch.

    Flow:
    1. Write the batch locally as JSONL.
    2. Compress the JSONL file using GZIP.
    3. Generate a partitioned S3 object key.
    4. Upload the compressed file to S3.
    5. Commit Kafka offsets only after upload succeeds.
    """

    if not event_batch:
        return

    logger.info(
        "Processing Bronze batch | records=%s",
        len(event_batch),
    )

    # Step 1: Write the batch into a unique local JSONL file.
    local_jsonl_file = write_events(event_batch)

    # Step 2: Compress the local JSONL file.
    compressed_file = compress_file(local_jsonl_file)

    if compressed_file is None:
        raise RuntimeError(
            f"Compression failed for file: {local_jsonl_file}"
        )

    # Step 3: Generate the partitioned S3 object key.
    s3_object_key = create_bronze_s3_key()

    logger.info(
        "Bronze S3 key generated | key=%s",
        s3_object_key,
    )

    # Step 4: Upload the compressed file to S3.
    upload_successful = upload_file_to_s3(
        local_file_path=compressed_file,
        bucket_name=S3_BUCKET_NAME,
        s3_object_key=s3_object_key,
        aws_region=AWS_REGION,
    )

    if not upload_successful:
        raise RuntimeError(
            f"S3 upload failed for file: {compressed_file}"
        )

    # Step 5: Commit only after the S3 upload succeeds.
    commit_batch_offsets(
        consumer=consumer,
        offsets_by_partition=batch_offsets,
    )

    logger.info(
        "Bronze batch uploaded and offsets committed | "
        "records=%s | s3_uri=s3://%s/%s | offsets=%s",
        len(event_batch),
        S3_BUCKET_NAME,
        s3_object_key,
        batch_offsets,
    )


def consume_booking_events() -> None:
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])

    event_batch: list[dict] = []

    # Example:
    # {
    #     0: 244,
    #     1: 180,
    #     2: 210
    # }
    #
    # The value represents the next Kafka offset to read.
    batch_offsets: dict[int, int] = {}

    logger.info(
        "Southwest booking consumer started | "
        "topic=%s | batch_size=%s | s3_bucket=%s",
        KAFKA_TOPIC,
        BATCH_SIZE,
        S3_BUCKET_NAME,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            # No message arrived during this poll.
            # The consumer remains alive and polls again.
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                logger.error(
                    "Kafka consumer error | error=%s",
                    message.error(),
                )
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )

            except (UnicodeDecodeError, json.JSONDecodeError):
                logger.exception(
                    "Invalid Kafka message | "
                    "partition=%s | offset=%s",
                    message.partition(),
                    message.offset(),
                )
                continue

            event_batch.append(event)

            partition = message.partition()

            # Kafka commits the next offset to read.
            # If offset 50 was processed, offset 51 is committed.
            next_offset = message.offset() + 1

            batch_offsets[partition] = max(
                batch_offsets.get(partition, 0),
                next_offset,
            )

            logger.info(
                "Event collected | batch=%s/%s | "
                "partition=%s | offset=%s | event_id=%s",
                len(event_batch),
                BATCH_SIZE,
                partition,
                message.offset(),
                event.get("event_id"),
            )

            if len(event_batch) >= BATCH_SIZE:
                process_batch(
                    consumer=consumer,
                    event_batch=event_batch,
                    batch_offsets=batch_offsets,
                )

                # Clear only after the complete process succeeds.
                event_batch.clear()
                batch_offsets.clear()

    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested")

    except Exception:
        logger.exception("Unexpected consumer failure")

    finally:
        # Process events that did not form a complete batch.
        if event_batch:
            try:
                process_batch(
                    consumer=consumer,
                    event_batch=event_batch,
                    batch_offsets=batch_offsets,
                )

                logger.info(
                    "Final partial batch processed successfully | "
                    "records=%s",
                    len(event_batch),
                )

            except Exception:
                logger.exception(
                    "Failed to process final partial batch | "
                    "records=%s",
                    len(event_batch),
                )

        consumer.close()
        logger.info("Kafka consumer stopped safely")


if __name__ == "__main__":
    consume_booking_events()