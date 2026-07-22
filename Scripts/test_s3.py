import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


BUCKET_NAME = "data-pipeline-southwest"
AWS_REGION = "us-east-1"


def test_s3_connection() -> None:
    """
    Verify that Python can connect to Amazon S3
    and confirm that our project bucket exists.
    """

    try:
        # Create an S3 client.
        # boto3 automatically reads the credentials saved by `aws configure`.
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

        # Ask AWS for the buckets available to this IAM user.
        response = s3_client.list_buckets()
       # print(response)

        # Extract only the bucket names from the AWS response.
        bucket_names = [
            bucket["Name"]
            for bucket in response.get("Buckets", [])
        ]

        print("Buckets available in this AWS account:")

        if not bucket_names:
            print("No S3 buckets were found.")
            return

        for bucket_name in bucket_names:
            print(f"- {bucket_name}")

        # Confirm that our Southwest project bucket exists.
        if BUCKET_NAME in bucket_names:
            print(
                f"\nConnection successful. "
                f"Bucket '{BUCKET_NAME}' was found."
            )
        else:
            print(
                f"\nPython connected to S3, but bucket "
                f"'{BUCKET_NAME}' was not found."
            )

    except NoCredentialsError:
        print(
            "AWS credentials were not found. "
            "Run 'aws configure' and try again."
        )

    except ClientError as error:
        error_code = error.response.get(
            "Error",
            {},
        ).get(
            "Code",
            "Unknown",
        )

        print(
            f"AWS rejected the S3 request. "
            f"Error code: {error_code}"
        )
        print(error)

    except BotoCoreError as error:
        print(f"boto3 could not communicate with AWS: {error}")

    except Exception as error:
        print(f"Unexpected error: {error}")


if __name__ == "__main__":
    test_s3_connection()