"""
Functions for fetching and storing information related to 'conts' signed between a 'bidder'
and a 'cauth' upon a 'tender', based in data from `www.contratación.euskadi.eus`.

Notes:
    · The following scrapper takes a huge amount of time to process files (around 50')
    · Async data fetching was implemented, but it was dismissed due to the problems the remote server
    presented while processing multiple requests.
"""
import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import requests

import scripts.utils.utils as utils
from e_cauths import get_cauth_dict_list
from scripts.transformers.t_cont import CONT_KNOWN_KEYS, XML_LIST_FIELDS, get_clean_cont
from scripts.utils import log

SCOPE = "conts"

TIME_STAMP = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.contratacion.euskadi.eus/"
CONT_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar/indicadorREST" \
                      "/descargarInformeOpenData" \
                      "?idioma=es" \
                      "&anio={report_date}" \
                      "&idPoder={codperfil}" \
                      "&R01HNoPortal=true"

CONT_BY_CAUTH_LIST_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                                    "/busquedaInformesOpenData" \
                                    "/tablaInformes/filter"


@log.start_end
def get_cont_file(scope_path):
    """
    Parses and cleans raw CONT `.xml` data and stores it in a CONT `.jsonl` file
    """
    jsonl_path = os.path.join(scope_path, f"conts.jsonl")
    with open(jsonl_path, 'w', encoding='utf-8') as jsonl:
        raw_cauth_conts_path = os.path.join(scope_path, 'raw_cauth_conts')
        # Iterating through every CONT directory
        for dirname in os.listdir(raw_cauth_conts_path):
            dir_path = os.path.join(raw_cauth_conts_path, dirname)
            cauth_cod_perfil, cont_date, cont_id, cont_mod_date = dirname.split('_')
            cauth_cod_perfil = str(int(cauth_cod_perfil))

            # Iterating through every CONT in a given `.xml` file
            if len(os.listdir(dir_path)) != 2:
                logging.info(f"Missing files for {dirname}")
                continue

            for xml_cont in ET.parse(os.path.join(dir_path, dirname + '.xml')).getroot():
                cont_d = {"cont_od_date_año": cont_date, "cont_cauth_cod_perfil": cauth_cod_perfil,
                          "cont_od_id": cont_id, "cont_od_date_fecha_mod": cont_mod_date}
                parsed_cont_d = utils.parse_xml_field(
                    node=xml_cont,
                    dict_obj=cont_d,
                    list_fields=XML_LIST_FIELDS,
                    pref2remove=['{com/ejie/ac70a/xml/opendata}', 'contratoOpenData']
                )
                parsed_cont_d["codExp"], *_ = parsed_cont_d["codContrato"].split('_')
                utils.check_no_matched_key(parsed_cont_d, CONT_KNOWN_KEYS)
                clean_cont = get_clean_cont(parsed_cont_d)
                jsonl.write(json.dumps(clean_cont, ensure_ascii=False) + '\n')
    return jsonl_path


@log.start_end
def get_raw_conts(scope_path):
    """ Fetch and store `.xml` and `.json` cont data files locally """
    # Iterating through every cauth contract report
    for cauth_d in get_cauth_dict_list():
        cauth_cod_perfil = cauth_d['codPerfil']
        # Iterating through every bidder CONT in a given list
        for cont in get_conts_by_cauth(cauth_cod_perfil=cauth_cod_perfil):
            # Parsing basic information
            cont_date = str(int(cont['anioInforme']))
            cont_mod_date = cont['fechaModif'].replace('-', '')
            # bidder CONT external id, as for the original website
            cont_ext_id = str(int(cont['idInformeOpendata']))
            # bidder CONT internal id, as for this project
            cont_int_id = f"{int(cauth_cod_perfil):05d}_{cont_date}_{cont_ext_id}_{cont_mod_date}"
            cont_fpath = os.path.join(scope_path, 'raw_cauth_conts', cont_int_id)

            # Check if data already exists (both the `.json` file and the `.xml` file)
            if os.path.isdir(cont_fpath) and len(os.listdir(cont_fpath)) == 2:
                continue
            else:
                os.makedirs(cont_fpath, exist_ok=True)

            # Store `.json` response
            json_fpath = os.path.join(cont_fpath, cont_int_id + '.json')
            with open(json_fpath, mode='w', encoding='utf8') as jsonfile:
                jsonfile.write(json.dumps(cont, indent=2, ensure_ascii=False))

            # Fetch and extract `.zip` file to store `.xml` file
            zip_url = CONT_URL.format(codperfil=cauth_cod_perfil, report_date=cont_date)
            get_xml_file(url=zip_url, cont_path=cont_fpath, cont_id=cont_int_id)
        break


@utils.retry(times=5, exceptions=json.decoder.JSONDecodeError, sleep=0.5)
def get_conts_by_cauth(cauth_cod_perfil):
    """ Fetch conts under a given cauth as a list"""
    payload = \
        {
            "length": 1000000,
            "filter": {
                "poder": {"codPerfil": cauth_cod_perfil},
                "anioDesde": "2000",
                "anioHasta": str(datetime.now().year)
            },
            "rows": 1000000,
            "page": 1
        }

    # Cookies to be used in the following request
    cookies_url = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                             "/busquedaInformesOpenData" \
                             "?locale=es"
    cookies = requests.get(cookies_url).headers['Set-Cookie']

    r_json = requests.post(CONT_BY_CAUTH_LIST_URL,
                           headers={'Cookie': cookies},
                           data=json.dumps(payload),
                           timeout=25).json()

    if int(r_json['page']) > 1:
        print("More data available than expected!")
        raise

    return r_json["rows"]


@utils.retry(times=5, exceptions=BadZipFile, sleep=10)
def get_xml_file(url, cont_path, cont_id):
    xml_fpath = os.path.join(cont_path, cont_id + '.xml')
    r = requests.get(url)
    if not r:
        return

    with ZipFile(BytesIO(r.content)) as zipfile:
        zipped_filenames = zipfile.namelist()
        if len(zipped_filenames) > 3:
            raise BadZipFile
            logging.critical(f"Malformed zip file for: {url}")

        for file in zipped_filenames:
            if file.endswith('.xml'):
                zipfile.extract(file, cont_path)
                os.rename(os.path.join(cont_path, file.replace('"', '_')), xml_fpath)


def get_conts(operation_date, path):
    scope_path = os.path.join(path, operation_date, SCOPE)
    # Get conts data
    get_raw_conts(scope_path)
    # Consolidate conts data
    jsonl_path = get_cont_file(scope_path)
    return jsonl_path


if __name__ == "__main__":
    DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data')
    os.makedirs(DATA_PATH, exist_ok=True)
    conts_jsonl_path = get_conts(operation_date=TIME_STAMP, path=DATA_PATH)
