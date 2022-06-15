"""
Functions for fetching and storing tender announcements (TENDs)
"""
import json
import logging
import os
from datetime import datetime, date

import requests

from e_utils import async_download_urls
from scripts.transformers.t_tenders.main import get_tenders_file
from scripts.utils import log
from scripts.utils.utils import get_hash

SCOPE = "tenders"

TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
YEARLY_TENDERS_URL = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{" \
                     "year}/opendata/contratos.json "


def get_raw_tenders_from_xmls(path):
    # Data paths
    json_tenders_path = os.path.join(path, 'raw_yearly_tenders')
    xml_tenders_path = os.path.join(path, 'raw_xml_tenders')
    os.makedirs(xml_tenders_path, exist_ok=True)
    # Iterate through yearly tenders json files
    rqfpath_list = []
    for json_fname in os.listdir(json_tenders_path):
        json_fpath = os.path.join(json_tenders_path, json_fname)
        tender_year = json_fname.split('_')[0]
        # Load JSON file and fix malformed files for certain years
        with open(json_fpath, encoding='utf-8', mode='r') as file:
            tenders = json.loads(file.read().removesuffix(');').removeprefix('jsonCallback('))
        # Iterate contracts in file
        for tender in tenders:
            if tender_year == '2018':
                data_xml_url = tender["xetrs89"]
            else:
                data_xml_url = tender["dataXML"]
            # As contracts do not have a pre-assigned code, create an ID
            xml_fpath = os.path.join(xml_tenders_path, f"{tender_year}_es_{get_hash(data_xml_url)[0:25]}.xml")
            # Append url and filepath to the list
            request_kwargs = {'url': data_xml_url, 'method': 'GET'}
            rqfpath_list.append((request_kwargs, xml_fpath))
    async_download_urls(rqfpath_list)


@log.start_end
def get_yearly_tends(path, start_year, end_year=date.today().year + 1):
    path = os.path.join(path, 'raw_yearly_tenders')
    os.makedirs(path, exist_ok=True)
    for year in range(start_year, end_year):
        # Retrieve response headers from `.json` datafile
        rh = requests.head(YEARLY_TENDERS_URL.format(year=year)).headers
        # Get `Last-Modified` date and `ETag` to name json file after them
        lastm = datetime.strftime(datetime.strptime(rh["Last-Modified"].split(',')[1], " %d %b %Y %X GMT"),
                                  "%Y%m%d%H%S%M")
        etag = rh['ETag'].replace('"', '')
        fname = f"{year}_{lastm}_{etag}.json"
        # Check if file is already there
        fpath = os.path.join(path, fname)
        if os.path.isfile(fpath):
            continue
        # Download and store `.json` datafile
        r = requests.get(YEARLY_TENDERS_URL.format(year=year))
        with open(fpath, mode="w", encoding='utf-8') as file:
            file.write(r.content.decode('utf-8'))
        logging.info(f"File '{fname}' fetched and stored.")


@log.start_end
def get_tenders(path):
    os.makedirs(DATA_PATH, exist_ok=True)
    # get_yearly_tends(path, start_year=2019)
    # get_raw_tenders_from_xmls(path)
    get_tenders_file(path)


if __name__ == "__main__":
    get_tenders(DATA_PATH)
