import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config.airport_master import AIRPORTS
from config.flight_master import FLIGHTS


def validate_flights() -> None:
    airport_codes = {
        airport["airport_code"]
        for airport in AIRPORTS
    }

    flight_numbers = [
        flight["flight_number"]
        for flight in FLIGHTS
    ]

    assert len(flight_numbers) == len(set(flight_numbers)), (
        "Flight numbers must be unique"
    )

    for flight in FLIGHTS:
        assert flight["origin"] in airport_codes, (
            f"Invalid origin airport: {flight['origin']}"
        )

        assert flight["destination"] in airport_codes, (
            f"Invalid destination airport: {flight['destination']}"
        )

        assert flight["origin"] != flight["destination"], (
            "Origin and destination cannot be the same"
        )

        assert flight["flight_number"].startswith("WN"), (
            "Southwest flight numbers should start with WN"
        )

        assert isinstance(flight["active_flag"], bool), (
            "active_flag must be True or False"
        )

    print("Flight master validation passed.")


if __name__ == "__main__":
    validate_flights()