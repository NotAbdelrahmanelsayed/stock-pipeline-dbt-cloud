from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
from utils.constants import logger


def initialize_gcs_client(service_account_path: Path) -> storage.Client:
    """
    Initialize a Google Cloud Storage client.

    Parameters
    ----------
    service_account_path : Union[str, Path]
        Path to the service account JSON file.

    Returns
    -------
    storage.Client
        GCS client instance.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        logger.info("GSC Credentials Initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Couldn't create a GCS client: {e}")
        raise


def upload_blob(
    client: storage.Client,
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str
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