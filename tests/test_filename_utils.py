import sys
from pathlib import Path
import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

CONFIG_TEXT = """
[google_cloud]
SERVICE_ACCESS_FILE_PATH=/tmp/fake.json
BUCKET_NAME=test-bucket
GSC_RAW_PATH=raw

[BigQuery]
PROJECT_ID=test-project
DATASET_NAME=test_dataset
TABLE_NAME=test_table

[y_finance]
STOCK_TICKERS=AAPL, MSFT

[data_pathes]
RAW_DIR_PATH=data/raw
RAW_CSV_FILE=raw_
"""

conf_path = ROOT / "config/configuration.conf"
conf_path.write_text(CONFIG_TEXT)

import utils.constants as constants


def teardown_module(module):
    if conf_path.exists():
        conf_path.unlink()


def test_generate_filename_default_order(monkeypatch):
    fake_now = datetime.datetime(2025, 1, 1)

    class FixedDate(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now

    monkeypatch.setattr(constants.datetime, "datetime", FixedDate)

    fname = constants.generate_filename()
    expected_start = (fake_now - datetime.timedelta(days=10 * 365)).strftime("%Y%m%d")
    expected_end = fake_now.strftime("%Y%m%d")
    assert fname == f"{constants.RAW_DATA_FILE_NAME}{expected_start}_{expected_end}.csv"

