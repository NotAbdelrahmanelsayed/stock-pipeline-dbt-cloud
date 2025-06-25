from workflows.yfinance_workflow import (
    extract_full_stock_prices,
    extract_incremental_stock_prices,
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


@patch("workflows.yfinance_workflow.save_data_locally")
@patch("workflows.yfinance_workflow.get_local_file_path")
@patch("workflows.yfinance_workflow.transform_stock_data")
@patch("workflows.yfinance_workflow.download_stock_data")
def test_extract_full_stock_prices(
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
    extract_full_stock_prices(mock_xcom)

    # Assert
    mock_download.assert_called_once_with(TICKERS, is_full_load=True)
    mock_transform.assert_called_once_with(raw_data)
    assert mock_get_path.called, "get_local_file_path(), is not called"
    mock_save_data.assert_called_once_with(transformed_data, filepath)


@patch("workflows.yfinance_workflow.save_data_locally")
@patch("workflows.yfinance_workflow.get_local_file_path")
@patch("workflows.yfinance_workflow.transform_stock_data")
@patch("workflows.yfinance_workflow.download_stock_data")
def test_extract_incremental_stock_prices(
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
    extract_incremental_stock_prices(mock_xcom)

    # Assert
    mock_download.assert_called_once_with(
        TICKERS, is_full_load=False, start_date=max_date
    )
    mock_transform.assert_called_once_with(raw_data)
    mock_get_path.assert_called_once_with(max_date)
    mock_save_data.assert_called_once_with(transformed_data, filepath)
