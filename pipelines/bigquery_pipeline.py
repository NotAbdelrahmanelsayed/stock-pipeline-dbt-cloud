from etl.bigquery_etl import initialize_bigquery_client, from_gsc_to_bigquery_table
from utils.constants import SERVICE_ACC_FILE, GSC_RAW_DATA_PATH, TABLE_ID, logger


def upload_to_bigquery(**kwargs):
    try:
        # Pull data_uri from Xcom
        ti = kwargs['ti']
        data_uri = ti.xcom_pull(task_ids='Upload_raw_data_to_GSC', key='data_uri')

        # Initialze BigQuery client
        bigquery_client = initialize_bigquery_client(SERVICE_ACC_FILE)
        # Upload the data from Google Cloud Storage to BigQuery 
        from_gsc_to_bigquery_table(bigquery_client, TABLE_ID, data_uri)
        logger.info(f"File {GSC_RAW_DATA_PATH} successfully uploaded to {TABLE_ID} from airflow")
    except Exception as e:
        logger.error(f"Airflow GSC upload failed :{e}")
        raise