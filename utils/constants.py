from utils.logging_config import setup_logging
from pathlib import Path
import configparser

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
SERVICE_ACC_FILE = config.get("google_cloud", "SERVICE_ACCESS_FILE_PATH")

# Google Bucket
BUCKET_NAME = config.get("google_cloud", "BUCKET_NAME")
GSC_RAW_DATA_PATH = config.get("data_pathes", "GSC_RAW_PATH")

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
