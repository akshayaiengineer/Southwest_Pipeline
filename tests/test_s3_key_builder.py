from utils.key_builder import create_bronze_s3_key


def test_create_bronze_s3_key() -> None:
    s3_key = create_bronze_s3_key()

    assert s3_key.startswith("bronze/year=")
    assert "/month=" in s3_key
    assert "/day=" in s3_key
    assert "/hour=" in s3_key
    assert s3_key.endswith(".jsonl.gz")
    assert "booking_events_" in s3_key
    print(s3_key)

test_create_bronze_s3_key()    