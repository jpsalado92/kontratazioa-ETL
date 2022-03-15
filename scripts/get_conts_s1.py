"""
En este módulo:
1.
https://opendata.euskadi.eus/catalogo/-/contrataciones-administrativas-del-2021/
"""
import random
from html.parser import HTMLParser
import re
import os
import time
import urllib3
import xml.etree.ElementTree
from datetime import datetime, date
import logging
import requests
import hashlib
# META_URI = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{year}/es_contracc/r01DCATDataset.rdf"
import json
import pprint

import xml.etree.ElementTree as ET
from html import unescape

LOGS_FILEPATH = os.path.join(os.getcwd(), 'logs', f'get_contratos-events.log')
DATA_PATH = os.path.join(os.getcwd(), 'data', 'raw_contracts')
YEARLY_JSON_CONTRACT_FIRST_YEAR = 2008
YEARLY_JSON_CONTRACT_DATA_URI = "https://opendata.euskadi.eus/contenidos/ds_contrataciones/contrataciones_admin_{year}/opendata/contratos.json"


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


def extend_contracts_information(year):
    year_data_path = os.path.join(DATA_PATH, str(year))

    # Among the local contracts json files for a given year, select the most recent one
    most_recent_filename = sorted(list(filter(lambda item: '.json' in item, os.listdir(year_data_path))))[0]
    json_contracts_filepath = os.path.join(year_data_path, most_recent_filename)

    # Load JSON file and fix malformed files for certain years
    with open(json_contracts_filepath, encoding='utf-8', mode='r') as f:
        contracts = json.loads(f.read().removesuffix(');').removeprefix('jsonCallback('))

    # Loop contract by contract
    # for contract in contracts:
    for attempt in range(1, 2):
        contract = random.choice(contracts)
        # As contracts do not have a pre-assigned code, create one from the hash of their properties
        h = hashlib.sha3_512()
        filename_literal = contract["documentName"] + contract["physicalUrl"]
        h.update(bytes(filename_literal, 'utf-8'))
        contract_filename = h.hexdigest()[1:20]

        # Create a folder to store extended information for each contract
        contract_data_path = os.path.join(json_contracts_filepath.removesuffix('.json'), contract_filename)
        os.makedirs(contract_data_path, exist_ok=True)

        # Store current contract information as an independent JSON file
        json_contract_filepath = os.path.join(contract_data_path, f"{contract_filename}.json")
        if not os.path.isfile(json_contract_filepath):
            with open(json_contract_filepath, "w", encoding='utf-8') as f:
                f.write(json.dumps(contract, indent=2, sort_keys=True, ensure_ascii=False))

        # Try fetching extended information as in "dataXML" URI
        xml_contract_filepath = os.path.join(contract_data_path, f"{contract_filename}.xml")
        get_xml_contract(contract['dataXML'], xml_contract_filepath)

        print(contract['dataXML'])
        if os.path.isfile(xml_contract_filepath):
            parse_xml_contract(xml_contract_filepath)


def get_yearly_json_contracts(year):
    """ Manage download of a json contract file from a given year """

    # Retrieve response headers from `.json` datafile
    rh = requests.head(YEARLY_JSON_CONTRACT_DATA_URI.format(year=year)).headers

    # Get `Last-Modified` date and `ETag` to name json file after them
    lastm = datetime.strftime(datetime.strptime(rh["Last-Modified"].split(',')[1], " %d %b %Y %X GMT"), "%Y%m%d%H%S%M")
    etag = rh['ETag'].replace('"', '')
    filename = f"{lastm}_{etag}.json"

    # Check if file already exists and return if so
    year_data_path = os.path.join(DATA_PATH, str(year))
    if os.path.isfile(os.path.join(DATA_PATH, filename)):
        return

    # Download and store `.json` datafile
    r = requests.get(YEARLY_JSON_CONTRACT_DATA_URI.format(year=year))
    os.makedirs(year_data_path, exist_ok=True)
    with open(os.path.join(year_data_path, filename), mode="w", encoding='utf-8') as f:
        f.write(r.content.decode('utf-8'))


def main():
    # Enable logging and avoid urllib3 related logging
    logging.basicConfig(filename=LOGS_FILEPATH, filemode='a', encoding='utf-8', level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.info(f"StartingLogAt:{datetime.now()}")
    for year in range(2021, 2022):
        # get_yearly_json_contracts(year)
        print(year)
        extend_contracts_information(year)

if __name__ == "__main__":
    main()
