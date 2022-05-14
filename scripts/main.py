import logging
import os
from datetime import datetime

import log
from get_conts_s1 import get_conts_s1

DATA_PATH = os.path.join(os.getcwd(), '..', 'data')

if __name__ == "__main__":
    # Date related to the current operation day
    op_date = datetime.now().strftime("%Y%m%d")

    # Directory in which data will be stored
    data_path = os.path.join(DATA_PATH, op_date)
    os.makedirs(data_path, exist_ok=True)

    # Enable logging
    log.start_log(data_path)
    logging.info(f"Starting log for: {op_date}")

    # Get adjt_conts data
    # adjt_conts_jsonl_path = get_adjt_conts(operation_date=op_date, path=DATA_PATH)

    # Get conts data
    conts_s1_jsonl_path = get_conts_s1(operation_date=op_date, path=DATA_PATH)
