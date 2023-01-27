import boto3
from airflow.models import Variable

# AWS creds
AWS_CREDS = {
    "AWS_ACCESS_KEY_ID": Variable.get("TOWER_DB_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": Variable.get("TOWER_DB_SECRET_ACCESS_KEY"),
}
# AWS region
AWS_REGION = "us-east-1"

def initialize_aws_client(resource: str, aws_creds: dict = AWS_CREDS, aws_region: str = AWS_REGION):
    """Initializes aws client

    Args:
        resource (str): resource type to be initialized
        aws_creds (dict): AWS access key and secret access key dictionary
        aws_region (str): Regoin for client to be initialized in
    """
    return boto3.client(
        resource,
        aws_access_key_id=aws_creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_creds["AWS_SECRET_ACCESS_KEY"],
        region_name=aws_region,
    )

def upload_file_s3(file_path: str, bucket_name: str) -> str:
    """Uploads file from file_path to s3 bucket_name

    Args:
        file_path (str): Path to file to be uploaded
        bucket_name (str): Location in S3 for file to be uploaded
    
    Returns:
        str: uri pointing to new uploaded file location in s3
    """
    s3_client = initialize_aws_client(resource="s3", aws_creds=AWS_CREDS, aws_region=AWS_REGION)
    file_name = file_path.split("/")[-1]
    s3_client.upload_file(file_path, bucket_name, file_name)
    return f"s3://{bucket_name}/{file_name}"
