import time

from producer.booking_event_generator import create_booking_event
from producer.kafka_producer import producer, publish_event
from utils.logger import get_logger


logger = get_logger("booking-producer")


def main() -> None:
    logger.info("Southwest Kafka producer started")

    published_count = 0

    try:
        while True:
            booking_event = create_booking_event()

            publish_event(booking_event)

            published_count += 1

            logger.info(
                "Booking event submitted | count=%s | event_id=%s | booking_id=%s",
                published_count,
                booking_event["event_id"],
                booking_event["payload"]["booking_id"],
            )

            time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Producer shutdown requested")

    except Exception:
        logger.exception("Unexpected producer failure")

    finally:
        remaining_messages = producer.flush(10)

        if remaining_messages == 0:
            logger.info("All queued Kafka messages were delivered")
        else:
            logger.warning(
                "%s Kafka message(s) were not delivered before shutdown",
                remaining_messages,
            )

        logger.info("Kafka producer stopped")


if __name__ == "__main__":
    main()  