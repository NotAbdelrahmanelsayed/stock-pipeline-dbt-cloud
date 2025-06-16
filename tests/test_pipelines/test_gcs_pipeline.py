from unittest.mock import patch, MagicMock, Mock
from pipelines.gcs_pipeline import upload_to_gcs
from utils.constants import BUCKET_NAME, GCS_RAW_DATA_PATH
import pytest
from pathlib import Path

@pytest.fixture
def blob_name():
    file_path = "fake/path"
    blob_name = f"{GCS_RAW_DATA_PATH}/{Path(file_path).name}"
    return blob_name

@patch("pipelines.gcs_pipeline.upload_blob")
@patch("pipelines.gcs_pipeline.create_bucket_if_not_exists")
@patch("pipelines.gcs_pipeline.initialize_gcs_client")
def test_upload_to_gcs(mock_client, mock_create_bucket, mock_upload_blob, blob_name):
    # Mock xcom pull
    mock_xcom = MagicMock()
    mock_xcom.xcom_pull.return_value = "fake/path"

    # Mock GCS client
    mock_gcs = Mock()
    mock_client.return_value = mock_gcs

    # Mock create bucket
    mock_create_bucket.return_value = True

    # Mock upload data to GCS
    mock_upload_blob.return_value = "gs://stock-pipeline-dbt-cloud/raw/raw_test.csv"

    upload_to_gcs(mock_xcom)

    assert mock_client.called, "initialize_gcs_client() is not called"
    mock_create_bucket.assert_called_once_with(mock_gcs, BUCKET_NAME), "initialize_gcs_client() is not called"
    mock_upload_blob.assert_called_once_with(mock_gcs, BUCKET_NAME, "fake/path", blob_name)