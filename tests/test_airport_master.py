import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)
from config.airport_master import AIRPORTS

def validate_airports() -> None:
    airport_codes = [
        airport["airport_code"]
        for airport in AIRPORTS
    ]
    assert len(AIRPORTS) == 7, "We expected exactly seven airports"
    assert len(airport_codes) == len(set(airport_codes)), (
        "Airport codes must be unique"
    )
    for airport in AIRPORTS:
        assert len(airport["airport_code"]) == 3, (
            "Airport code must contain three characters"
        )
        assert airport["country"] == "USA", (
            "All initial airports should be in the USA"
        )
        assert airport["timezone"], (
            "Timezone cannot be empty"
        )

    print("Airport master validation passed.")


if __name__ == "__main__":
    validate_airports()