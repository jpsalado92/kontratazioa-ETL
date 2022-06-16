import logging
import os
from datetime import datetime

from utils import log
from extractors.e_cauths import get_cauths
from extractors.e_conts import get_conts
from extractors.e_tenders import get_tenders
from extractors.e_bidders import get_bidders

DATA_PATH = os.path.join(os.getcwd(), '..', 'data')


def main():
    # Date related to the current operation day
    op_date = datetime.now().strftime("%Y%m%d")

    # Directory in which data will be stored
    data_path = os.path.join(DATA_PATH, op_date)
    os.makedirs(data_path, exist_ok=True)

    # Enable logging
    log.start_log(os.path.join(DATA_PATH, op_date))
    logging.info(f"Starting log for: {op_date}")

    # Trigger pipelines
    get_cauths(os.path.join(DATA_PATH, op_date, 'cauths'))
    get_conts(os.path.join(DATA_PATH, op_date, 'conts'))
    get_bidders(os.path.join(DATA_PATH, op_date, 'bidders'))
    get_tenders(os.path.join(DATA_PATH, op_date, 'tenders'))


if __name__ == "__main__":
    main()
