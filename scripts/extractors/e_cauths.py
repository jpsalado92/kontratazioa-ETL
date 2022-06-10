"""
Functions for fetching and storing data related to `cauth` (contracting authority) entities
from `www.contratación.euskadi.eus`

Possible exceptions:

PA_EXCEPTIONS = ["311", "231", "24446", "24589"]
    "codPerfil": "311" is a mistake, it should be "codPerfil": "612"
    "codPerfil": "231" is broken
    "codPerfil": "24446" is still not published
"""

import logging
import os
from datetime import datetime

import requests

from scripts.transformers.t_cauth import parse_htmls
from scripts.utils.utils import del_none, strip_dict

SCOPE = "cauths"
TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
BASE_URL = "https://www.contratacion.euskadi.eus/"
CAUTH_DETAIL_URL = BASE_URL + "w32-kpeperfi/es/contenidos/poder_adjudicador/"
CAUTH_DETAIL_URL_V1 = CAUTH_DETAIL_URL + "poder{codPerfil}/es_doc/es_arch_poder{codPerfil}.html"
CAUTH_DETAIL_URL_V2 = CAUTH_DETAIL_URL + "poder{codPerfil}/es_doc/index.html"


def get_cauth_dict_list() -> list:
    """ Returns a list of cauth entities (dicts) as in contratacion.euskadi.eus """
    cauth_list_url = BASE_URL + "ac70cPublicidadWar/busquedaInformesOpenData/" \
                                "autocompleteObtenerPoderes?q= "
    cauth_json = requests.get(cauth_list_url).json()
    cauths = [strip_dict(del_none(cauth)) for cauth in cauth_json]
    logging.info(f"Number of PAs fetched: {len(cauths)} ")
    return cauths


def get_cauth_dict() -> dict:
    """ Returns a dict containing cauths `codPerfil` as keys and cauth entities as values """
    cauths_d = {}
    for cauth_d in get_cauth_dict_list():
        cauths_d[cauth_d["codPerfil"]] = cauth_d
    return cauths_d


def get_raw_cauth_htmls():
    """ Fetches and stores raw html data from cauths listed with `get_cauth_dict_list()` """
    path = os.path.join(DATA_PATH, 'raw_html')
    try:
        os.makedirs(path, exist_ok=False)
        for cauth_d in get_cauth_dict_list():
            cauth_cod_perfil = cauth_d['codPerfil']
            # Store raw html content
            v1_url = CAUTH_DETAIL_URL_V1.format(codPerfil=cauth_cod_perfil)
            raw_html = requests.get(v1_url).content.decode('ISO-8859-1')
            if "imagen de error 404" not in raw_html:
                version = "v1"
            else:
                v2_url = CAUTH_DETAIL_URL_V2.format(codPerfil=cauth_cod_perfil)
                raw_html = requests.get(v2_url).content.decode('ISO-8859-1')
                version = "v2"
            # Manage local filepath
            filename = '_'.join((version, cauth_cod_perfil)) + '.html'
            filepath = os.path.join(path, filename)
            # Store raw html
            with open(filepath, mode='w', encoding='ISO-8859-1') as file:
                file.write(raw_html)
    except:
        logging.info(f"Data available at {path}")


if __name__ == "__main__":
    os.makedirs(DATA_PATH, exist_ok=True)
    get_raw_cauth_htmls()
    parse_htmls(data_path=DATA_PATH, cauth_dict=get_cauth_dict())
