import json
import logging
import os

from bs4 import BeautifulSoup

from scripts.utils import log
from scripts.transformers.t_tenders.p_cann import parse_contracting_announcement_xml
from scripts.transformers.t_tenders.p_record import parse_record_xml


@log.start_end
def get_tenders_file(path):
    """ Parses and cleans raw TENDER `.xml` data and stores it in a TENDER `.jsonl` file """
    jsonl_path = os.path.join(path, 'tenders.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as jsonl:
        raw_tenders_path = os.path.join(path, 'raw_xml_tenders')
        # Iterating through every TENDER xml
        for xml_filename in os.listdir(raw_tenders_path):
            odr_year = xml_filename.split('_')[0]
            with open(os.path.join(raw_tenders_path, xml_filename), mode='r', encoding='utf8') as file:
                xml_file = file.read().replace('encoding="ISO-8859-1"', 'encoding="utf8"')
                soup = BeautifulSoup(xml_file, 'xml')
            try:
                if soup.find('record'):
                    clean_tender = parse_record_xml(soup)
                elif soup.find('contractingAnnouncement'):
                    clean_tender = parse_contracting_announcement_xml(soup)
                else:
                    logging.warning(f"No header match for file: {xml_filename}")
                    continue
                full_tender = clean_tender | {'odr_year': odr_year}
                jsonl.write(json.dumps(full_tender, ensure_ascii=False) + '\n')
            except (TypeError, AttributeError) as e:
                logging.warning(f"Could not process {xml_filename}, {e}")
                pass
