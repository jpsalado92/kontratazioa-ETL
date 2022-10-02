"""
Functions for fetching and storing information related to 'conts' entities signed between a 'bidder'
and a 'cauth' upon a 'tender'.

Notes:
    · The following scrapper takes a huge amount of time to process files (around 50')
    · Async data fetching was implemented, but it was dismissed due to the problems the remote server
    presented handling multiple requests.
"""
import json
import logging
import os
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import requests

import src.utils.utils as utils
from src.extractors.e_cauths import get_cauth_dict_list
from src.transformers.t_conts import get_conts_file
from src.utils import log

SCOPE = "conts"
TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
BASE_URL = "https://www.contratacion.euskadi.eus/"

CONT_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar/indicadorREST" \
                      "/descargarInformeOpenData" \
                      "?idioma=es" \
                      "&anio={report_year}" \
                      "&idPoder={codperfil}" \
                      "&R01HNoPortal=true"

CONT_BY_CAUTH_LIST_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                                    "/busquedaInformesOpenData" \
                                    "/tablaInformes/filter"


@utils.retry(times=5, exceptions=BadZipFile, sleep=10)
def get_xml_from_zip_url(url, cont_path, xml_fname):
    xml_fpath = os.path.join(cont_path, xml_fname)
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


@utils.retry(times=5, exceptions=json.decoder.JSONDecodeError, sleep=0.5)
def get_yearly_conts_by_cauth(cauth_cod_perfil):
    payload = {"length": 1000000, "filter": {"poder": {"codPerfil": cauth_cod_perfil}, "anioDesde": "2000",
        "anioHasta": str(datetime.now().year)}, "rows": 1000000, "page": 1}
    # Cookies to be used in the following request
    cookies_url = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                             "/busquedaInformesOpenData" \
                             "?locale=es"
    cookies = requests.get(cookies_url).headers['Set-Cookie']
    r_json = requests.post(CONT_BY_CAUTH_LIST_URL, headers={'Cookie': cookies}, data=json.dumps(payload),
                           timeout=25).json()
    if int(r_json['page']) > 1:
        logging.warning("More data available than expected!")
        raise
    return r_json["rows"]


@log.start_end
def get_raw_cont_xmls(path):
    # Iterating through every cauth contract report
    for cauth_d in get_cauth_dict_list():
        cauth_cod_perfil = cauth_d['codPerfil']
        # Iterating through every bidder CONT in a given list
        for yearly_od_report in get_yearly_conts_by_cauth(cauth_cod_perfil=cauth_cod_perfil):
            od_report_year = str(int(yearly_od_report['anioInforme']))
            od_report_date_modified = yearly_od_report['fechaModif'].replace('-', '')
            od_report_id = str(int(yearly_od_report['idInformeOpendata']))
            xml_fname = f"{int(cauth_cod_perfil):05d}_{od_report_year}_{od_report_id}_{od_report_date_modified}.xml"
            xml_fpath = os.path.join(path, 'raw_cauth_conts')
            if not os.path.isfile(os.path.join(xml_fpath, xml_fname)):
                zip_url = CONT_URL.format(codperfil=cauth_cod_perfil, report_year=od_report_year)
                get_xml_from_zip_url(url=zip_url, cont_path=xml_fpath, xml_fname=xml_fname)


@log.start_end
def get_conts(path):
    os.makedirs(path, exist_ok=True)
    get_raw_cont_xmls(path)
    get_conts_file(path)


if __name__ == "__main__":
    get_conts(DATA_PATH)
