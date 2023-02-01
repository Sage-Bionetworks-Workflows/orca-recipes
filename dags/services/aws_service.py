from pathlib import Path

import boto3
from airflow.models import Variable


class AWSService:
    def __init__(self, config_name):
        # AWS creds
        AWS_CREDS_CONFIG = {
            "tower_db_prod": {
                "AWS_ACCESS_KEY_ID": Variable.get("TOWER_DB_ACCESS_KEY"),
                "AWS_SECRET_ACCESS_KEY": Variable.get("TOWER_DB_SECRET_ACCESS_KEY"),
            },
            "tower_db_dev": {  # TODO add new secrets when it is time to switch to prod account
                "AWS_ACCESS_KEY_ID": Variable.get("TOWER_DB_ACCESS_KEY"),
                "AWS_SECRET_ACCESS_KEY": Variable.get("TOWER_DB_ACCESS_KEY"),
            },
        }

        self.AWS_ACCESS_KEY = AWS_CREDS_CONFIG[config_name]["AWS_ACCESS_KEY"]
        self.AWS_SECRET_ACCESS_KEY = AWS_CREDS_CONFIG[config_name][
            "AWS_SECRET_ACCESS_KEY"
        ]
        # AWS region
        self.AWS_REGION = "us-east-1"

    def initialize_aws_client(self, resource: str):
        """Initializes aws client

        Args:
            resource (str): resource type to be initialized
        """
        return boto3.client(
            resource,
            aws_access_key_id=self.AWS_ACCESS_KEY,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_REGION,
        )

    def upload_file_s3(self, file_path: Path, bucket_name: str) -> str:
        """Uploads file from file_path to s3 bucket_name

        Args:
            file_path (str): Path to file to be uploaded
            bucket_name (str): Location in S3 for file to be uploaded

        Returns:
            str: uri pointing to new uploaded file location in s3
        """
        s3_client = self.initialize_aws_client(resource="s3")
        s3_client.upload_file(file_path, bucket_name, file_path.name)
        return f"s3://{bucket_name}/{file_path.name}"
