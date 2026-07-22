import json
import os
import sys

from confluent_kafka import Consumer, KafkaError, TopicPartition


# Add the project root to Python's import path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)


from producer.file_writer import write_events
from utils.logger import get_logger


logger = get_logger("booking-consumer")


KAFKA_TOPIC = "southwest-booking-events"
BATCH_SIZE = 10

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
    Commit the next offset to read for every partition
    included in the successfully written batch.
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
    # The value is the next offset Kafka should read.
    batch_offsets: dict[int, int] = {}

    logger.info(
        "Southwest booking consumer started | topic=%s | batch_size=%s",
        KAFKA_TOPIC,
        BATCH_SIZE,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            # No new message arrived during the one-second poll.
            # The consumer stays alive and polls again.
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
                    "Invalid Kafka message | partition=%s | offset=%s",
                    message.partition(),
                    message.offset(),
                )

                # We are not committing this malformed message yet.
                # Later, we will write bad messages to quarantine/DLQ.
                continue

            event_batch.append(event)

            partition = message.partition()

            # Kafka commits the next offset to read.
            # If offset 50 was processed, commit offset 51.
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
                # First write the data safely to Bronze.
                write_events(event_batch)

                # Commit only after the Bronze write succeeds.
                commit_batch_offsets(
                    consumer=consumer,
                    offsets_by_partition=batch_offsets,
                )

                logger.info(
                    "Batch written and offsets committed | "
                    "records=%s | offsets=%s",
                    len(event_batch),
                    batch_offsets,
                )

                event_batch.clear()
                batch_offsets.clear()

    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested")

    except Exception:
        logger.exception("Unexpected consumer failure")

    finally:
        # Write any remaining events that did not fill a complete batch.
        if event_batch:
            try:
                write_events(event_batch)

                commit_batch_offsets(
                    consumer=consumer,
                    offsets_by_partition=batch_offsets,
                )

                logger.info(
                    "Final partial batch written and committed | "
                    "records=%s | offsets=%s",
                    len(event_batch),
                    batch_offsets,
                )

            except Exception:
                logger.exception(
                    "Failed to write or commit final partial batch"
                )

        consumer.close()
        logger.info("Kafka consumer stopped safely")


if __name__ == "__main__":
    consume_booking_events()