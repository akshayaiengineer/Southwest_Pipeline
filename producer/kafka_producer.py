import json
from confluent_kafka import Producer
from utils.logger import get_logger
logger = get_logger("kafka-producer")

KAFKA_TOPIC = "southwest-booking-events"

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(KAFKA_CONFIG)


def delivery_report(error, message) -> None:
    if error is not None:
        logger.error(
            "Kafka delivery failed | error=%s",
            error,
        )
        return

    logger.info(
        "Kafka delivery successful | topic=%s | partition=%s | offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def publish_event(event: dict) -> None:
    event_json = json.dumps(event)
    booking_id = event["payload"]["booking_id"]

    producer.produce(
        topic=KAFKA_TOPIC,
        key=booking_id,
        value=event_json,
        callback=delivery_report,
    )

    producer.poll(0)