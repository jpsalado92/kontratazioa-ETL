import logging
import os
from datetime import datetime

from scripts.utils import log

DATA_PATH = os.path.join(os.getcwd(), '..', 'data')


def main():
    # Date related to the current operation day
    op_date = datetime.now().strftime("%Y%m%d")

    # Directory in which data will be stored
    data_path = os.path.join(DATA_PATH, op_date)
    os.makedirs(data_path, exist_ok=True)

    # Enable logging
    log.start_log(data_path)
    logging.info(f"Starting log for: {op_date}")


if __name__ == "__main__":
    main()
