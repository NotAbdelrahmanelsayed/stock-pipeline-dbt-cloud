"""
GCS client init, file upload, and bucket creation.
"""

from google.cloud import storage
from utils.constants import logger


def upload_blob(
    client: storage.Client,
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str,
) -> str:
    """
    Upload a file to a GCS bucket.

    Parameters
    ----------
    client : storage.Client
        GCS client object.
    bucket_name : str
        Name of the GCS bucket.
    source_file_name : str
        Path to the file to upload.
    destination_blob_name : str
        Name for the blob in GCS.

    Returns
    -------
    str
        The GCS URI of the uploaded file.
        Example: 'gs://stock-pipeline-dbt-cloud/raw/raw_test.csv'
    """
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        data_uri = f"gs://{bucket_name}/{destination_blob_name}"
        logger.info(f"Uploaded {source_file_name} to {data_uri}")
        return data_uri
    except Exception as e:
        logger.error(f"Failed to upload blob: {e}")
        raise


def create_bucket_if_not_exists(
    client: storage.Client,
    bucket_name: str,
    location: str = "US",
    storage_class: str = "STANDARD",
) -> bool:
    """
    Create a GCS bucket if it does not already exist.

    Parameters
    ----------
    client : storage.Client
        GCS client object.
    bucket_name : str
        Name of the GCS bucket.
    location : str
        GCS bucket location (default is 'US').
    storage_class : str
        Storage class for the bucket (default is 'STANDARD').

    Returns
    -------
    bool
        True if the bucket was created or already exists, False otherwise.
    """

    try:
        bucket = client.lookup_bucket(bucket_name)
        if bucket:
            logger.info(f"Bucket `{bucket_name}` already exists.")
            return True
    except Exception as e:
        logger.error(f"Error looking up bucket `{bucket_name}`: {e}")
        raise

    try:
        bucket = client.bucket(bucket_name)
        bucket.location = location
        bucket.storage_class = storage_class
        new_bucket = client.create_bucket(bucket)

        logger.info(
            f"Created bucket `{new_bucket.name}` in location `{new_bucket.location}` "
            f"with storage class `{new_bucket.storage_class}`."
        )
        return True
    except Exception as e:
        logger.error(f"Failed to create bucket `{bucket_name}`: {e}")
        return False
