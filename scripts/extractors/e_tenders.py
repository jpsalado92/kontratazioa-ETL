"""
Functions for fetching and storing tender announcements (TENDs)
from `www.contratación.euskadi.eus`.
"""
import asyncio
import json
import logging
import os
import sys
from asyncio import Semaphore
from datetime import datetime, date
from typing import IO

import aiofile
import aiohttp
import requests
from aiohttp import ClientSession, ClientTimeout

from scripts.utils import log
from scripts.utils.utils import get_hash

SCOPE = "tenders"

TIME_STAMP = datetime.now().strftime("%Y%m%d")
RAW_YEARLY_TENDERS_DIR = 'raw_yearly_tenders'
RAW_TENDER_XML_DIR = 'raw_xml_tenders'
YEARLY_JSON_TENDER_URL = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{year}/opendata/contratos.json"


@log.start_end
def get_tenders_file(scope_path):
    """
    Parses and cleans raw TENDER `.xml` data and stores it in a TENDER `.jsonl` file
    """
    jsonl_path = os.path.join(scope_path, f"{SCOPE}.jsonl")
    with open(jsonl_path, 'w', encoding='utf-8') as jsonl:
        raw_tenders_path = os.path.join(scope_path, RAW_TENDER_XML_DIR)
        # Iterating through every TENDER xml
        for xml_filename in os.listdir(raw_tenders_path):
            return NotImplemented


async def get_html(url: str, session: ClientSession, **kwargs) -> str:
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    html = resp.text()
    return await html


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    attempt = 1
    times = 5
    while attempt < times + 1:
        try:
            html = await get_html(url, session, **kwargs)
        except (
                aiohttp.ClientError,
                aiohttp.http_exceptions.HttpProcessingError,
        ) as e:
            message = getattr(e, 'message', None)
            status = getattr(e, 'status', None)
            if status != 404:
                attempt += 1
            else:
                logging.warning(f"aiohttp exception for {url} [{status}]: {message}")
                return
        except asyncio.exceptions.TimeoutError as e:
            attempt += 1
            logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
        except Exception as e:
            logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
            return
        else:
            return html
    logging.warning(f'Unable to succesfully get {url} after {times}. [{status}]: {message}')
    return


async def write_one(file: IO, url: str, sem: Semaphore, **kwargs) -> None:
    async with sem:
        html = await fetch_html(url=url, **kwargs)
        if not html:
            return None
        async with aiofile.AIOFile(file, 'w') as fl:
            await fl.write(html)


async def get_tenders_xmls(urls):
    timeout = ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=100, force_close=True)
    sem = Semaphore(100)
    async with ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for url, file in urls:
            tasks.append(write_one(file=file, url=url, session=session, sem=sem))
        await asyncio.gather(*tasks)
    return


def get_urls_from_yearly_tenders(scope_path):
    # Data paths
    json_tenders_path = os.path.join(scope_path, RAW_YEARLY_TENDERS_DIR)
    xml_tenders_path = os.path.join(scope_path, RAW_TENDER_XML_DIR)
    os.makedirs(xml_tenders_path, exist_ok=True)
    # Iterate through yearly tenders json files
    urls = []
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
            xml_fpath = os.path.join(xml_tenders_path, f"{tender_year}_es_{get_hash(data_xml_url)}")
            # Append url and filepath to the url list
            urls.append((data_xml_url, xml_fpath))
    return urls


@log.start_end
def get_yearly_tends(scope_path, start_year, end_year=date.today().year + 1):
    raw_yearly_tenders_path = os.path.join(scope_path, RAW_YEARLY_TENDERS_DIR)
    os.makedirs(raw_yearly_tenders_path, exist_ok=True)
    for year in range(start_year, end_year):
        # Retrieve response headers from `.json` datafile
        rh = requests.head(YEARLY_JSON_TENDER_URL.format(year=year)).headers
        # Get `Last-Modified` date and `ETag` to name json file after them
        lastm = datetime.strftime(
            datetime.strptime(rh["Last-Modified"].split(',')[1], " %d %b %Y %X GMT"), "%Y%m%d%H%S%M")
        etag = rh['ETag'].replace('"', '')
        filename = f"{year}_{lastm}_{etag}.json"
        # Check if file is already there
        filepath = os.path.join(raw_yearly_tenders_path, filename)
        if os.path.isfile(filepath):
            continue
        # Download and store `.json` datafile
        r = requests.get(YEARLY_JSON_TENDER_URL.format(year=year))
        with open(filepath, mode="w", encoding='utf-8') as file:
            file.write(r.content.decode('utf-8'))
        logging.info(f"File '{filename}' fetched and stored.")


def get_tenders(operation_date, path):
    scope_path = os.path.join(path, operation_date, SCOPE)
    # Get yearly tender data
    get_yearly_tends(scope_path, start_year=2022)
    # Retrieve urls to download tenders
    urls = get_urls_from_yearly_tenders(scope_path)
    logging.info(f"Number of tenders to be downloaded: {len(urls)}")
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_tenders_xmls(urls))
    # Consolidate CONTs data
    # get_tenders_file(scope_path)


if __name__ == "__main__":
    DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data')
    os.makedirs(DATA_PATH, exist_ok=True)
    get_tenders(operation_date=TIME_STAMP, path=DATA_PATH)
