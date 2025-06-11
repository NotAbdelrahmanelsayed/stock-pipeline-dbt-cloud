import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    ShortCircuitOperator,
)
from datetime import datetime
from pipelines.yfinance_pipeline import (
    download_full_stock_data,
    download_delta_stock_data,
)
from pipelines.gcs_pipeline import upload_to_gcs
from pipelines.bigquery_pipeline import (
    upload_to_bigquery,
    initialize_bigquery_client,
    SERVICE_ACCOUNT_FILE,
)
from utils.bigquery_helpers import get_last_loaded_date
from utils.data_validation import check_data_quality
from utils.constants import TABLE_ID
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {"owner": "Abdelrahman", "start_date": datetime(2025, 6, 1)}

dag = DAG(
    dag_id="daily_stock_etl",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
)

decide_load_type = BranchPythonOperator(
    task_id="decide_load_type",
    python_callable=get_last_loaded_date,
    op_kwargs={
        "client": initialize_bigquery_client(SERVICE_ACCOUNT_FILE),
        "table_id": TABLE_ID,
    },
    dag=dag,
)

extract_all_stock_data = PythonOperator(
    task_id="extract_stock_data_full", python_callable=download_full_stock_data, dag=dag
)

delta_extract = PythonOperator(
    task_id="extract_stock_data_delta",
    python_callable=download_delta_stock_data,
    dag=dag,
)

upload_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs", python_callable=upload_to_gcs, dag=dag
)

upload_to_bigquery_task = PythonOperator(
    task_id="gcs_to_bigquery", python_callable=upload_to_bigquery, dag=dag
)

check_staged_data = ShortCircuitOperator(
    task_id="validate_staged_data",
    ignore_downstream_trigger_rules=True,
    python_callable=check_data_quality,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

decide_load_type >> extract_all_stock_data >> check_staged_data
decide_load_type >> delta_extract >> check_staged_data

check_staged_data >> upload_to_gcs_task >> upload_to_bigquery_task
