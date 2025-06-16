from pipelines.yfinance_pipeline import (
    download_full_stock_data,
    download_delta_stock_data,
)
from unittest.mock import patch, MagicMock, Mock
from utils.constants import TICKERS


def setup_mocks(
    mock_download,
    mock_transform,
    mock_get_path,
    mock_save_data,
    raw_data,
    transformed_data,
    filepath,
):
    mock_download.return_value = raw_data
    mock_transform.return_value = transformed_data
    mock_get_path.return_value = filepath
    mock_save_data.return_value = True


@patch("pipelines.yfinance_pipeline.save_data_locally")
@patch("pipelines.yfinance_pipeline.get_local_file_path")
@patch("pipelines.yfinance_pipeline.transform_stock_data")
@patch("pipelines.yfinance_pipeline.download_stock_data")
def test_download_full_stock_data(
    mock_download,
    mock_transform,
    mock_get_path,
    mock_save_data,
    raw_data,
    transformed_data,
    filepath,
):
    """Test that full stock data is downloaded, transformed, and saved as expected."""

    # Mock xcom_push
    mock_xcom = MagicMock()

    setup_mocks(
        mock_download,
        mock_transform,
        mock_get_path,
        mock_save_data,
        raw_data,
        transformed_data,
        filepath,
    )

    # Act
    download_full_stock_data(mock_xcom)

    # Assert
    mock_download.assert_called_once_with(TICKERS, is_full_load=True)
    mock_transform.assert_called_once_with(raw_data)
    assert mock_get_path.called, "get_local_file_path(), is not called"
    mock_save_data.assert_called_once_with(transformed_data, filepath)


@patch("pipelines.yfinance_pipeline.save_data_locally")
@patch("pipelines.yfinance_pipeline.get_local_file_path")
@patch("pipelines.yfinance_pipeline.transform_stock_data")
@patch("pipelines.yfinance_pipeline.download_stock_data")
def test_download_delta_stock_data(
    mock_download,
    mock_transform,
    mock_get_path,
    mock_save_data,
    raw_data,
    transformed_data,
    filepath,
):
    """Test that delta stock data is downloaded, transformed, and saved as expected."""
    # Mock xcom_pull
    mock_xcom = MagicMock()
    max_date = "2025-06-15"
    mock_xcom.xcom_pull.return_value = max_date

    setup_mocks(
        mock_download,
        mock_transform,
        mock_get_path,
        mock_save_data,
        raw_data,
        transformed_data,
        filepath,
    )

    # Act
    download_delta_stock_data(mock_xcom)

    # Assert
    mock_download.assert_called_once_with(
        TICKERS, is_full_load=False, start_date=max_date
    )
    mock_transform.assert_called_once_with(raw_data)
    mock_get_path.assert_called_once_with(max_date)
    mock_save_data.assert_called_once_with(transformed_data, filepath)
