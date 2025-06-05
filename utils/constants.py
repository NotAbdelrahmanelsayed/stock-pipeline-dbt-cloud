from utils.logging_config import setup_logging
from pathlib import Path
import configparser
import datetime


# Prepare the logger
logger = setup_logging()

# Getting the config parser
config = configparser.ConfigParser()
conf_path = Path(__file__).resolve().parents[1] / "config/configuration.conf"

if not conf_path.exists():
    logger.error(f"Configuration path: {conf_path} doesn't exist")
    raise
config.read(conf_path)


# Google Credentials
SERVICE_ACCOUNT_FILE = config.get("google_cloud", "SERVICE_ACCESS_FILE_PATH")

# Google Bucket
BUCKET_NAME = config.get("google_cloud", "BUCKET_NAME")
GCS_RAW_DATA_PATH = config.get("google_cloud", "GSC_RAW_PATH")

# BigQuery
PROJECT_ID = config.get("BigQuery", "PROJECT_ID")
DATASET_NAME = config.get("BigQuery", "DATASET_NAME")
DATASET_ID = f"{PROJECT_ID}.{DATASET_NAME}"
TABLE_NAME = config.get("BigQuery", "TABLE_NAME")
TABLE_ID = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"


# Tickers labels
TICKERS = config.get('y_finance','STOCK_TICKERS').split(', ')


# Data Dir handeling
RAW_DATA_PATH = Path(config.get("data_pathes", "RAW_DIR_PATH")).resolve() # Full path
RAW_DATA_FILE_NAME = config.get("data_pathes", "RAW_CSV_FILE")
RAW_DATA_FILE_PATH = RAW_DATA_PATH / RAW_DATA_FILE_NAME

def generate_filename(start_date: datetime.datetime | None = None) -> str:
    """Return a standardized raw CSV filename.

    Parameters
    ----------
    start_date : datetime.datetime | None
        The first date in the filename. If ``None`` the start date is set
        to ten years prior to today and the end date is set to today.

    Returns
    -------
    str
        Filename using the pattern ``RAW_DATA_FILE_NAMEYYYYMMDD_YYYYMMDD.csv``.
    """

    if start_date is None:
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=10 * 365)
    else:
        end_date = datetime.datetime.now()

    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    return f"{RAW_DATA_FILE_NAME}{start}_{end}.csv"


def get_local_file_path(start_date: datetime.datetime | None = None) -> str:
    """Return the full local path for a raw CSV file."""

    return str(RAW_DATA_PATH / generate_filename(start_date))


def get_gcs_blob_path(start_date: datetime.datetime | None = None) -> str:
    """Return the blob path used when uploading to GCS."""

    return f"{GCS_RAW_DATA_PATH}/{generate_filename(start_date)}"