import random
import uuid
from datetime import datetime, timedelta, timezone

from config.flight_master import FLIGHTS
def create_booking_event() -> dict:
    flight = random.choice(FLIGHTS)

    flight_date = (
        datetime.now(timezone.utc).date()
        + timedelta(days=random.randint(1, 30))
    )

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "BOOKING_CREATED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "southwest-booking-app",
        "schema_version": "1.0",
        "payload": {
            "booking_id": str(uuid.uuid4()),
            "customer_id": f"C-{random.randint(1000, 9999)}",
            "flight_number": flight["flight_number"],
            "flight_date": str(flight_date),
            "origin": flight["origin"],
            "destination": flight["destination"],
            "fare": round(random.uniform(75, 650), 2),
            "currency": "USD",
            "booking_status": "PENDING_PAYMENT",
        },
    }
