import pandas as pd
import great_expectations as gx
from utils.constants import logger, TICKERS
from great_expectations.datasource.fluent.interfaces import Batch

EXPECTED_COLUMNS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]


def get_batch(file_path: str) -> Batch:
    """Read the data through GX and return a GX batch for validation"""
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Failed to read CSV: {file_path}", exc_info=True)
        raise

    # Create data context to manage configurations
    context = gx.get_context()

    if "pandas_data_source" not in context.data_sources:
        data_source = context.data_sources.add_pandas("pandas_data_source")
    else:
        data_source = context.data_sources["pandas_data_source"]

    if "raw_stock_data" not in data_source.assets:
        data_asset = data_source.add_dataframe_asset(name="raw_stock_data")
    else:
        data_asset = data_source.assets["raw_stock_data"]

    batch_def = data_asset.add_batch_definition_whole_dataframe("raw_stock_stage")
    return batch_def.get_batch(batch_parameters={"dataframe": df})

def get_expectation_suite() -> gx.ExpectationSuite:
    """ Defines the expectations suite

    Returns
    -------
    gx.ExpectationSuite
        gx expectation suite with expectations
    """
    suite = gx.ExpectationSuite(name="raw_stock_suite")

    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=EXPECTED_COLUMNS,
            exact_match=True
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=len(TICKERS),
            max_value=1_000_000_0
        )
    )

    for col in EXPECTED_COLUMNS:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )
    
    suite.add
    return suite


def check_data_quality(**kwargs) -> bool:
    """Check the quality of the staged data file before uploading it to GCS or BigQuery."""
    
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='extract_stock_data_delta', key='file_name') \
                 or ti.xcom_pull(task_ids='extract_stock_data_full', key='file_name')
    
    # Get the data batch
    batch = get_batch(file_path)
    
    # Get expectations suite
    suite = get_expectation_suite()
    
    # Validate
    validation_result = batch.validate(suite)

    if not validation_result.success:
        logger.warning(f"Validation failed:\n{validation_result.describe()}")
        return False

    logger.info("Data validation passed successfully.")
    return True
