import logging
import os
from datetime import datetime

from extractors.e_bidders import get_bidders
from extractors.e_cauths import get_cauths
from extractors.e_conts import get_conts
from extractors.e_tenders import get_tenders
from loaders.elasticsearch.l_elasticsearch import load_in_es
from utils import log

DATA_PATH = os.path.join(os.getcwd(), '..', 'data')
CAUTH_ID = 'cauths'
CONT_ID = 'conts'
BIDDER_ID = 'bidders'
TENDER_ID = 'tenders'

def main():
    # Date related to the current operation day
    op_date = datetime.now().strftime("%Y%m%d")

    # Directory in which data will be stored
    data_path = os.path.join(DATA_PATH, op_date)
    os.makedirs(data_path, exist_ok=True)

    # Enable logging
    log.start_log(os.path.join(DATA_PATH, op_date))
    logging.info(f"Starting log for: {op_date}")

    # Declare project paths
    cauths_path = os.path.join(DATA_PATH, op_date, CAUTH_ID)
    conts_path = os.path.join(DATA_PATH, op_date, CONT_ID)
    bidders_path = os.path.join(DATA_PATH, op_date, BIDDER_ID)
    tenders_path = os.path.join(DATA_PATH, op_date, TENDER_ID)

    # Trigger ET pipelines
    get_cauths(cauths_path)
    get_conts(conts_path)
    get_bidders(bidders_path)
    get_tenders(tenders_path)

    # Load to ES
    load_in_es(
        (
            (cauths_path, CAUTH_ID),
            (conts_path, CONT_ID),
            (bidders_path, BIDDER_ID),
            (tenders_path, TENDER_ID),
        )
    )


if __name__ == "__main__":
    main()
