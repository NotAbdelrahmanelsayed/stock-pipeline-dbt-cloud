from pipelines.bigquery_pipeline import upload_to_bigquery
from unittest.mock import patch, MagicMock, Mock
from utils.constants import DATASET_ID, TABLE_ID


@patch("pipelines.bigquery_pipeline.create_dataset_if_not_exists")
@patch("pipelines.bigquery_pipeline.from_gsc_to_bigquery_table")
@patch("pipelines.bigquery_pipeline.initialize_bigquery_client")
def test_upload_to_bigquery(
    mock_bq_connection, mock_from_gsc_to_bq, mock_create_dataset
):
    # Mock xcom pull
    mock_xcom = MagicMock()
    mock_xcom.xcom_pull.return_value = "fake/path"

    # Mock BigQuery Connection
    mock_bq = Mock()
    mock_bq_connection.return_value = mock_bq

    # Mock create dataset behaviour
    mock_create_dataset.return_value = True

    # Mock load to bigquery from gcs
    mock_from_gsc_to_bq.return_value = True

    upload_to_bigquery(mock_xcom)

    assert mock_bq_connection.called, "initialize_bigquery_client() is not called"
    mock_create_dataset.assert_called_once_with(
        mock_bq, DATASET_ID
    ), "create_dataset_if_not_exists() is not called"
    mock_from_gsc_to_bq.assert_called_once_with(
        mock_bq, TABLE_ID, "fake/path"
    ), "from_gsc_to_bigquery_table() is not called"
