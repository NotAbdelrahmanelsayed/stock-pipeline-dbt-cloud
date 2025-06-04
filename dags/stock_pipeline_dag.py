import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pipelines.yfinance_pipeline import initial_download_stock_data
from pipelines.gcs_pipeline import upload_to_bigquery, upload_to_gcs

default_args = {
    "owner": "Abdelrahman",
    "start_date": datetime(2025, 6, 1)
}

dag = DAG(
    dag_id= "etl_stock_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
)

# Extract Data from yfinance and save it locally
extract = PythonOperator(
    task_id="Initial_extraction",
    python_callable=initial_download_stock_data,
    dag=dag
    )

load_gsc = PythonOperator(
    task_id="Upload_raw_data_to_GSC",
    python_callable=upload_to_gcs,
    dag=dag
)

load_bigquery = PythonOperator(
    task_id="Upload_raw_data_from_gsc_to_BigQuery",
    python_callable=upload_to_bigquery,
    dag=dag
)
extract >> load_gsc >> load_bigquery