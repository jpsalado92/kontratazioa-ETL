"""
Functions for fetching and storing tender announcements (TENDs)
from `www.contratación.euskadi.eus`.
"""
import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
from asyncio import Semaphore
import aiofile
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import random
from html.parser import HTMLParser
import re
import os
import time
import urllib3
import xml.etree.ElementTree
from datetime import datetime, date
import log
import logging
import requests
import hashlib
# META_URI = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{year}/es_contracc/r01DCATDataset.rdf"
import json
import pprint

import xml.etree.ElementTree as ET
from html import unescape
import utils
SCOPE = "get_tenders"

# YEARLY_JSON_CONTRACT_FIRST_YEAR Should be lower than 2011 as, based on cont_s1_kvs,
# there is no "contratacion_expediente" key for files before that date. Also, as there are
# no adjt_conts before 2015, that date is set
RAW_YEARLY_TENDERS_DIR = '1_raw_yearly_tenders'
RAW_TENDER_XML_DIR = '2_raw_xml_tenders'
TEND_JSON_FIRST_YEAR = 2022
YEARLY_TEND_JSON_URL = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{year}/opendata/contratos.json"


class MyHTMLParser(HTMLParser):
    """ Used to decode html text"""
    def __init__(self):
        HTMLParser.__init__(self)
        self.reset()
        self.HTMLDATA = []

    def handle_starttag(self, tag, attrs):
        pass
    def handle_endtag(self, tag):
        pass
    def handle_data(self, data):
        self.HTMLDATA = data


def clean_xml_text(text):
    """ Process xml data """
    # Handle html encoded data
    text = unescape(text)
    parser = MyHTMLParser()
    parser.feed(text)
    text = parser.HTMLDATA

    # Handle line breaks
    text = text.replace('\n', '').replace('\xa0', ' ')

    # Handle € values
    text = text.replace('euros', '€').replace('EUROS', '€').replace(' €', '').replace('€', '')

    # Handle numbers
    try:
        text = float(text.replace('.', '').replace(',', '.'))
    except:
        pass

    # Handle dates
    try:
        text = datetime.strftime(datetime.strptime(text, "%d/%m/%Y"), "%Y-%m-%d")
    except:
        pass

    # Handle `false` values
    if text in ('N', 'No'):
        return False

    # Handle `true` values
    if text in ('S', 'Si'):
        return True

    # Handle `null` values
    if text in ('',):
        return None

    return text


def xml_parser_type_1(node, d, path=None):
    """
    v1_parser
    Los item tienen siempre un atributo, que marca la key.
    Los item pueden tener varios values.
    Los values pueden tener varios items, pero no repetidos.
    Los value pueden ser un texto o varios items.

    Implementación:
    Root. Primer nivel tiene varios items.
        Los items son distintos.
            Iterar sobre los hijos de root

        Hay items duplicados.
            Separar hijos de root duplicados
            Separar hijos de root no duplicados
    """
    if node.tag == 'item':
        item = node
        item_name = item.attrib['name']
        new_path = '-'.join((path, item_name))
        if len(item) >= 2:
            container = []
            for value_child in item:
                dd = {}
                xml_parser_type_1(value_child, dd, '')
                container.append(dd.copy())
            d[new_path] = container
        else:
            for value_child in item:
                xml_parser_type_1(value_child, d, new_path)

    elif node.tag == 'value':
        value = node
        if value.text and value.text.replace('\n', ''):
            d[path] = clean_xml_text(value.text)
        else:
            for item_child in value:
                xml_parser_type_1(item_child, d, path)

    else:
        for child in node:
            xml_parser_type_1(child, d, node.tag)


def parse_xml_contract(filepath):
    if not os.path.isfile(filepath):
        return

    filename = filepath.split(os.sep)[-2]
    year = filepath.split(os.sep)[-4]

    root = ET.parse(filepath).getroot()
    # URL = "http://opendata.euskadi.eus/contenidos/contratacion/co_53_08/es_co_06_07/data/es_r01dpd01195238155c137f0c0dc0e232c3f2753bc"
    # root = ET.fromstring(requests.get(URL).content)
    parsed_xml = {}
    xml_parser_type_1(root, parsed_xml)
    pprint.pprint(parsed_xml)
    with open('prueba.json', "w", encoding='utf-8') as f:
        f.write(json.dumps(parsed_xml, indent=2, sort_keys=True, ensure_ascii=False))


def get_xml_contract(uri, filepath):
    # Return if file already exists
    if os.path.isfile(filepath):
        return

    # Check if attempt is cataloged as NotParsable or FailedRequest in log file
    filename = filepath.split(os.sep)[-2]
    year = filepath.split(os.sep)[-4]
    with open(LOGS_FILEPATH, "r") as lf:
        lf = lf.read()
    if f'Cant parse xml for {filename} file in {year}!' in lf:
        print(f"NotParsableAttempt found for {filename}")
        return
    elif f'FailedRequest:{filename}' in lf:
        print(f"FailedRequest found for {filename}")
        return

    # Try fetching xml file, beware of failed connection and try after 10 seconds if so
    try:
        r = requests.get(uri)
    except (ConnectionError, ConnectionResetError, TimeoutError, urllib3.exceptions.ProtocolError) as e:
        print('Unable to fetch file retrying after 10 seconds...')
        time.sleep(10)
        r = requests.get(uri)

    # If the connection did not succeed log it and return
    if not r.ok:
        print(f"Failed request {r.status_code} for {filename} in {year}")
        logging.info(f"FailedRequest:{filename}")
        return

    # Try storing a parseable xml file, log it if it is not parseable
    try:
        _ = ET.fromstring(r.content)
        with open(filepath, mode="w", encoding='iso-8859-1') as f:
            f.write(r.content.decode('iso-8859-1'))
    except xml.etree.ElementTree.ParseError as e:
        print(f'Cant parse xml for {filename} file in {year}!')
        logging.info(f'Cant parse xml for {filename} file in {year}!')


# @log.start_end
def extend_contracts_information(scope_path):
    raw_conts_path = os.path.join(scope_path, RAW_PATH)
    for yearly_cont_filename in os.listdir(raw_conts_path):
        json_contracts_filepath = os.path.join(raw_conts_path, yearly_cont_filename)

        # Load JSON file and fix malformed files for certain years
        with open(json_contracts_filepath, encoding='utf-8', mode='r') as file:
            contracts = json.loads(file.read().removesuffix(');').removeprefix('jsonCallback('))

        for contract in contracts:
            return
        # for attempt in range(1, 2):
        #     contract = random.choice(contracts)


            # Store current contract information as an independent JSON file
            # json_contract_filepath = os.path.join(contract_data_path, f"{contract_filename}.json")
            # if not os.path.isfile(json_contract_filepath):
            #     with open(json_contract_filepath, "w", encoding='utf-8') as f:
            #         f.write(json.dumps(contract, indent=2, sort_keys=True, ensure_ascii=False))

            # Try fetching extended information as in "dataXML" URI
            # xml_contract_filepath = os.path.join(contract_data_path, f"{contract_filename}.xml")
            # get_xml_contract(contract['dataXML'], xml_contract_filepath)
            #
            # print(contract['dataXML'])
            # if os.path.isfile(xml_contract_filepath):
            #     parse_xml_contract(xml_contract_filepath)
        print(f"{yearly_cont_filename} has {num_conts} contracts.")
        print(json.dumps(d, indent=2))


@log.start_end
def get_yearly_tends_jsons(scope_path, start_year=TEND_JSON_FIRST_YEAR, end_year=date.today().year + 1):
    raw_yearly_tenders_path = os.path.join(scope_path, RAW_YEARLY_TENDERS_DIR)
    os.makedirs(raw_yearly_tenders_path, exist_ok=True)
    for year in range(start_year, end_year):
        # Retrieve response headers from `.json` datafile
        rh = requests.head(YEARLY_TEND_JSON_URL.format(year=year)).headers
        # Get `Last-Modified` date and `ETag` to name json file after them
        lastm = datetime.strftime(datetime.strptime(rh["Last-Modified"].split(',')[1], " %d %b %Y %X GMT"), "%Y%m%d%H%S%M")
        etag = rh['ETag'].replace('"', '')
        filename = f"{year}_{lastm}_{etag}.json"
        # Check if file is already there
        filepath = os.path.join(raw_yearly_tenders_path, filename)
        if os.path.isfile(filepath):
            continue
        # Download and store `.json` datafile
        r = requests.get(YEARLY_TEND_JSON_URL.format(year=year))
        with open(filepath, mode="w", encoding='utf-8') as file:
            file.write(r.content.decode('utf-8'))
        logging.info(f"File '{filename}' fetched and stored.")


def get_urls_from_yearly_tenders(scope_path):
    urls = []
    # Data paths
    xml_tenders_path = os.path.join(scope_path, RAW_TENDER_XML_DIR)
    os.makedirs(xml_tenders_path, exist_ok=True)
    json_tenders_path = os.path.join(scope_path, RAW_YEARLY_TENDERS_DIR)

    # Iterate through yearly tender json files
    for json_fname in os.listdir(json_tenders_path):
        json_fpath = os.path.join(json_tenders_path, json_fname)
        tender_year = json_fname.split('_')[0]
        # Load JSON file and fix malformed files for certain years
        with open(json_fpath, encoding='utf-8', mode='r') as file:
            tenders = json.loads(file.read().removesuffix(');').removeprefix('jsonCallback('))

        # Iterate contracts
        for tender in tenders:
            if tender_year == '2018':
                data_xml_url = tender["xetrs89"]
            else:
                data_xml_url = tender["dataXML"]

            # As contracts do not have a pre-assigned code, create one from the hash of their "contratacion_expediente" pa+cod?
            h = hashlib.sha3_512()
            filename_literal = data_xml_url
            h.update(bytes(filename_literal, 'utf-8'))
            tender_filename = f"{tender_year}_es_{h.hexdigest()}"
            xml_fpath = os.path.join(xml_tenders_path, tender_filename)
            urls.append((data_xml_url, xml_fpath))
    return urls


def get_tenders(operation_date, path):
    scope_path = os.path.join(path, operation_date, SCOPE)
    # Get TENDs data
    # get_yearly_tends_jsons(scope_path, start_year=2008, end_year=2023)
    urls = get_urls_from_yearly_tenders(scope_path)
    import time
    t0 = time.time()
    print(len(urls))
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_tenders_xmls(urls))
    print(time.time()- t0)
    # Consolidate CONTs data
    # jsonl_path = extend_contracts_information(scope_path)
    # return jsonl_path


async def get_html(url: str, session: ClientSession, **kwargs) -> str:
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    # print("Got response [%s] for URL: %s", resp.status, url)
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
                # time.sleep(1)
            else:
                # logging.warning(f"aiohttp exception for {url} [{status}]: {message}")
                print(f"aiohttp exception for {url} [{status}]: {message}")
                return
        except asyncio.exceptions.TimeoutError as e:
            attempt += 1
            # time.sleep(1)
            # logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
            # print(f"asyncio.exceptions.TimeoutError exception occured for  {url} [{e}]")
        except Exception as e:
            # logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
            print(f"asyncio.exceptions.TimeoutError exception occured for  {url} [{e}]")
            return
        else:
            return html

    # logging.warning(f'Unable to succesfully get {url} after {times}. [{status}]: {message}')
    print(f'Unable to fetch {url} after {times} attempts.')
    return


async def write_one(file: IO, url: str, sem:Semaphore, **kwargs) -> None:
    async with sem:
        html = await fetch_html(url=url, **kwargs)
        if not html:
            return None
        # with open(file, "w", encoding='utf-8') as f:
        #     f.write(html)
            # print("Wrote results for source URL: %s", url)
        async with aiofile.AIOFile(file, 'w', encoding='utf-8') as fl:
            await fl.write(html)


async def get_tenders_xmls(urls):
    timeout = ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=100, force_close=True)
    sem = Semaphore(100)
    async with ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for url, file in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, sem=sem)
            )
        await asyncio.gather(*tasks)

    return

if __name__ == "__main__":
    DATA_PATH = os.path.join(os.getcwd(), '../..', 'data')
    op_date = datetime.now().strftime("%Y%m%d")
    tenders_jsonl_path = get_tenders(operation_date=op_date, path=DATA_PATH)
