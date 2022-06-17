"""
Functions for fetching and storing data related to `CAUTH` (contracting authority) entities
"""

import logging
import os
from datetime import datetime

import requests

from scripts.transformers.t_cauths import get_cauths_file
from scripts.transformers.t_utils import del_none, strip_dict
from scripts.utils import log

SCOPE = "cauths"
TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
BASE_URL = "https://www.contratacion.euskadi.eus/"
CAUTH_URL = BASE_URL + "w32-kpeperfi/es/contenidos/poder_adjudicador/"
CAUTH_URL_V1 = CAUTH_URL + "poder{codPerfil}/es_doc/es_arch_poder{codPerfil}.html"
CAUTH_URL_V2 = CAUTH_URL + "poder{codPerfil}/es_doc/index.html"


def get_cauth_dict_list(verbose=False) -> list:
    """ Returns a list of CAUTH entities (dicts) """
    cauth_list_url = BASE_URL + "ac70cPublicidadWar/busquedaInformesOpenData/" \
                                "autocompleteObtenerPoderes?q= "
    cauth_json = requests.get(cauth_list_url).json()
    cauths = [strip_dict(del_none(cauth)) for cauth in cauth_json]
    if verbose:
        logging.info(f"Number of PAs fetched: {len(cauths)} ")
    return cauths


def get_cauth_dict() -> dict:
    """ Returns a dict containing CAUTHs `codPerfil` as keys and CAUTH entities as values """
    cauths_d = {}
    for cauth_d in get_cauth_dict_list():
        cauths_d[cauth_d["codPerfil"]] = cauth_d
    return cauths_d


@log.start_end
def get_raw_cauth_htmls(path):
    """ Fetches and stores raw html data from CAUTHs listed with `get_cauth_dict_list()` """
    path = os.path.join(path, 'raw_html')
    os.makedirs(path, exist_ok=True)
    for cauth_d in get_cauth_dict_list(verbose=True):
        cauth_cod_perfil = cauth_d['codPerfil']
        # Store raw html content
        v1_url = CAUTH_URL_V1.format(codPerfil=cauth_cod_perfil)
        raw_html = requests.get(v1_url).content.decode('ISO-8859-1')
        if "imagen de error 404" not in raw_html:
            version = "v1"
        else:
            v2_url = CAUTH_URL_V2.format(codPerfil=cauth_cod_perfil)
            raw_html = requests.get(v2_url).content.decode('ISO-8859-1')
            version = "v2"
        # Manage local filepath
        filename = '_'.join((version, cauth_cod_perfil)) + '.html'
        filepath = os.path.join(path, filename)
        # Store raw html
        with open(filepath, mode='w', encoding='ISO-8859-1') as file:
            file.write(raw_html)


@log.start_end
def get_cauths(path):
    os.makedirs(path, exist_ok=True)
    get_raw_cauth_htmls(path)
    get_cauths_file(path, get_cauth_dict())


if __name__ == "__main__":
    get_cauths(DATA_PATH)
