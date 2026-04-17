from boto3 import client
from botocore.client import Config

from .base_storage import BaseStorageService
from ..config import settings
from ..logger import logger


def get_minio_s3_client():
    """
    Create and return a Boto3 S3 client configured for MinIO Object Storage.
    """
    logger.info("Creating MinIO S3 client")
    s3_client = client(
        "s3",
        endpoint_url=settings.storage_endpoint,
        aws_access_key_id=settings.storage_access_key,
        aws_secret_access_key=settings.storage_secret_key,
        region_name=settings.storage_region,
        config=Config(signature_version="s3v4"),
    )
    logger.info("MinIO S3 client created successfully: %s", s3_client)
    return s3_client


def create_minio_s3_bucket(bucket_name: str) -> None:
    """
    Create a new bucket in MinIO Object Storage.

    :param bucket_name: Name of the bucket to create.
    """
    logger.info("Creating bucket: %s", bucket_name)
    s3_client = get_minio_s3_client()
    s3_client.create_bucket(Bucket=bucket_name)
    logger.info("Bucket created successfully: %s", bucket_name)


def upload_file_to_minio_s3(file_path: str, bucket_name: str, object_name: str) -> None:
    """
    Upload a file to MinIO Object Storage.

    :param file_path: Path to the file to upload.
    :param bucket_name: Name of the MinIO S3 bucket.
    :param object_name: S3 object name (key) under which to store the file.
    """
    logger.info(
        "Uploading file %s to bucket %s as %s", file_path, bucket_name, object_name
    )
    s3_client = get_minio_s3_client()
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info("File uploaded successfully: %s", file_path)
    except Exception as e:
        logger.error(
            "Error uploading file %s to bucket %s: %s", file_path, bucket_name, e
        )
        raise e


def download_file_from_minio_s3(
    bucket_name: str, object_name: str, file_path: str
) -> None:
    """
    Download a file from MinIO Object Storage.

    :param bucket_name: Name of the MinIO S3 bucket.
    :param object_name: S3 object name (key) to download.
    :param file_path: Local path where the downloaded file will be saved.
    """
    logger.info(
        "Downloading file %s from bucket %s to %s", object_name, bucket_name, file_path
    )
    s3_client = get_minio_s3_client()
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
        logger.info("File downloaded successfully: %s", file_path)
    except Exception as e:
        logger.error(
            "Error downloading file %s from bucket %s: %s", object_name, bucket_name, e
        )
        raise e


def save_results_to_minio_s3(results: str, bucket_name: str, object_name: str) -> None:
    """
    Save results (e.g., JSON string) to MinIO Object Storage.

    :param results: The results data to save (e.g., JSON string).
    :param bucket_name: Name of the MinIO S3 bucket.
    :param object_name: S3 object name (key) under which to store the results.
    """
    logger.info("Saving results to bucket %s as %s", bucket_name, object_name)
    s3_client = get_minio_s3_client()
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=results)
        logger.info("Results saved successfully to %s/%s", bucket_name, object_name)
    except Exception as e:
        logger.error(
            "Error saving results to bucket %s as %s: %s", bucket_name, object_name, e
        )
        raise e


def list_minio_s3_objects(bucket_name: str, prefix: str = "") -> list:
    """
    List objects in a MinIO S3 bucket.

    :param bucket_name: Name of the MinIO S3 bucket.
    :param prefix: Optional prefix to filter objects by key.
    :return: List of object keys in the bucket that match the prefix.
    """
    logger.info("Listing objects in bucket %s with prefix '%s'", bucket_name, prefix)
    s3_client = get_minio_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        object_keys = [obj["Key"] for obj in objects]
        logger.info(
            "Found %d objects in bucket %s with prefix '%s'",
            len(object_keys),
            bucket_name,
            prefix,
        )
        return object_keys
    except Exception as e:
        logger.error(
            "Error listing objects in bucket %s with prefix '%s': %s",
            bucket_name,
            prefix,
            e,
        )
        raise e


def delete_minio_s3_object(bucket_name: str, object_name: str) -> None:
    """
    Delete an object from MinIO Object Storage.

    :param bucket_name: Name of the MinIO S3 bucket.
    :param object_name: S3 object name (key) to delete.
    """
    logger.info("Deleting object %s from bucket %s", object_name, bucket_name)
    s3_client = get_minio_s3_client()
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        logger.info("Object deleted successfully: %s", object_name)
    except Exception as e:
        logger.error(
            "Error deleting object %s from bucket %s: %s", object_name, bucket_name, e
        )
        raise e


class MinioStorageService(BaseStorageService):
    def __init__(self, layer: str = "bronze"):
        self.s3_client = get_minio_s3_client()
        self.bucket_name = (
            settings.bronze_bucket if layer == "bronze" else settings.silver_bucket
        )

    def upload_file(self, file_path: str, destination_path: str) -> None:
        upload_file_to_minio_s3(file_path, self.bucket_name, destination_path)

    def download_file(self, source_path: str, destination_path: str) -> str:
        download_file_from_minio_s3(self.bucket_name, source_path, destination_path)
        return destination_path

    def save_results(self, results: str, object_name: str) -> None:
        save_results_to_minio_s3(results, self.bucket_name, object_name)

    def list_files(self, directory_path: str = "") -> list:
        return list_minio_s3_objects(self.bucket_name, prefix=directory_path)

    def delete_file(self, file_path: str) -> None:
        delete_minio_s3_object(self.bucket_name, object_name=file_path)
