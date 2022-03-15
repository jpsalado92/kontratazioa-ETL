import logging
import os
from datetime import datetime

import log
from get_adjt_conts import get_raw_cont_files, get_cont_file

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

    # Fetch adjt_conts data
    get_raw_cont_files(operation_date=op_date, path=DATA_PATH)

    # Consolidate adjt_conts data
    get_cont_file(operation_date=op_date, path=DATA_PATH)
