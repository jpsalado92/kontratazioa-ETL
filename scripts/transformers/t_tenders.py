import json
import os
import pprint
import xml.etree.ElementTree as ET
from datetime import datetime
from html import unescape
from html.parser import HTMLParser


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
