from utils.logger import get_logger
from utils.s3_uploader import upload_file_to_s3


logger = get_logger("s3-upload-test")


def main() -> None:
    success = upload_file_to_s3(
        local_file_path="data/bronze/booking_events.jsonl",
        bucket_name="data-pipeline-southwest",
        s3_object_key="bronze/test/booking_events.jsonl",
    )

    if success:
        logger.info("S3 upload test passed")
    else:
        logger.error("S3 upload test failed")


if __name__ == "__main__":
    main()