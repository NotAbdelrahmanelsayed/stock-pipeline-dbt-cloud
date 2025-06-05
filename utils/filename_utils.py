from utils.constants import RAW_DATA_FILE_NAME
import datetime

def generate_filename(start_date=None):
    if not start_date:
        start_date = datetime.datetime.now()
        end_date = start_date - datetime.timedelta(10*365)
    else:
        end_date = datetime.datetime.now()
        
    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    return f"{RAW_DATA_FILE_NAME}{start}_{end}.csv"