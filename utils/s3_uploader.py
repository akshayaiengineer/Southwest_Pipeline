from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from utils.logger import get_logger

logger = get_logger("s3-uploader")

def upload_file_to_s3(
    local_file_path: str,
    bucket_name: str,
    s3_object_key: str,
    aws_region: str = "us-east-1",
) -> bool:
    file_path = Path(local_file_path)

    if not file_path.exists():
        logger.error(
            "Local file was not found | path=%s",
            file_path,
        )
        return False

    try:
        s3_client = boto3.client(
            "s3",
            region_name=aws_region,
        )

        s3_client.upload_file(
            Filename=str(file_path),
            Bucket=bucket_name,
            Key=s3_object_key,
        )

        logger.info(
            "File uploaded successfully | local_file=%s | s3_uri=s3://%s/%s",
            file_path,
            bucket_name,
            s3_object_key,
        )

        return True

    except NoCredentialsError:
        logger.error("AWS credentials were not found")
        return False

    except ClientError:
        logger.exception(
            "AWS rejected the S3 upload | bucket=%s | key=%s",
            bucket_name,
            s3_object_key,
        )
        return False

    except BotoCoreError:
        logger.exception(
            "boto3 failed during upload | bucket=%s | key=%s",
            bucket_name,
            s3_object_key,
        )
        return False

    except Exception:
        logger.exception(
            "Unexpected S3 upload failure | bucket=%s | key=%s",
            bucket_name,
            s3_object_key,
        )
        return False