"""
Functions for fetching and storing data related to `PAs` (poderes adjudicadores)
from `www.contratación.euskadi.eus`

Exceptions:

PA_EXCEPTIONS = ["311", "231", "24446"]
    "codPerfil": "311" is really "codPerfil": "612"
    "codPerfil": "231" is broken
    "codPerfil": "24446" is still not published

Useful resources:

    · What is a PA?:
    https://www.contratacion.euskadi.eus/informacion-general-poderes-adjudicadores/w32-kpeapa/es/

    · PA finder:
    https://www.contratacion.euskadi.eus/w32-kpeperfi/es/ac70cPublicidadWar/busquedaPoderAdjudicador?idioma=es

    · Sample detail url (v1):
    https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/poder_adjudicador/poder443/es_doc/es_arch_poder443.html
    https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/poder_adjudicador/poder44/es_doc/es_arch_poder44.html

    · Sample detail url (v2):
    https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/poder_adjudicador/poder24519/es_doc/index.html

Other available resources from xml document:
    · CPV
    · Medio propio
    · Poderes adjudicadores
    · IICS
    · Órgano de recurso
"""
import json
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from utils import del_none, strip_dict
import logging
SCOPE = "poderes_adjudicadores"
DATA_PATH = os.path.join(os.getcwd(), '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.contratacion.euskadi.eus/"
PA_DETAIL_URL = BASE_URL + "w32-kpeperfi/es/contenidos/poder_adjudicador/"
PA_DETAIL_URL_V1 = PA_DETAIL_URL + "poder{codPerfil}/es_doc/es_arch_poder{codPerfil}.html"
PA_DETAIL_URL_V2 = PA_DETAIL_URL + "poder{codPerfil}/es_doc/index.html"


def get_pa_dict_list() -> list:
    """ Returns a list of PA entities (dicts) as in contratacion.euskadi.eus """
    pa_list_url = BASE_URL + "ac70cPublicidadWar/busquedaInformesOpenData/" \
                             "autocompleteObtenerPoderes?q= "
    pa_json = requests.get(pa_list_url).json()
    pas = [strip_dict(del_none(pa)) for pa in pa_json]
    logging.info(f"Number of PAs fetched: {len(pas)} ")
    return pas


def get_pa_dict() -> dict:
    """ Returns a dict containing PAs`codPerfil` as keys and PA entities as values """
    pas_d = {}
    for pa_d in get_pa_dict_list():
        pas_d[pa_d["codPerfil"]] = pa_d
    return pas_d


def get_pa_detail():
    """ Fetches and stores raw html data from PAs listed with `get_pa_dict_list` """
    for pa_d in get_pa_dict_list():
        pa_cod_perfil = pa_d['codPerfil']

        # Store raw html content
        v1_url = PA_DETAIL_URL_V1.format(codPerfil=pa_cod_perfil)
        raw_html = requests.get(v1_url).content.decode('ISO-8859-1')
        if not raw_html == "<!-- Recurso no encontrado -->\n":
            version = "v1"
        else:
            v2_url = PA_DETAIL_URL_V2.format(codPerfil=pa_cod_perfil)
            raw_html = requests.get(v2_url).content.decode('ISO-8859-1')
            version = "v2"

        # Manage local directory path
        path = os.path.join(DATA_PATH, 'raw_html', TIME_STAMP)
        os.makedirs(path, exist_ok=True)

        # Manage local filepath
        filename = '_'.join((version, pa_cod_perfil)) + '.html'
        filepath = os.path.join(path, filename)

        # Store raw html
        with open(filepath, mode='w', encoding='ISO-8859-1') as file:
            file.write(raw_html)


def parse_htmls(date: str, pa_dict: dict):
    """
    Based on raw html data retrieved by `get_pa_detail()`, generates a PA
    consolidated jsonl file at DATA_PATH

    :param date: Date used to indicate the location of raw html data. '20220225' like format
    :param pa_dict: Dict built with `get_pa_dict()`
    """

    cfilename = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE + '.jsonl')))
    with open(cfilename, mode='w', encoding='utf8') as cfile:
        html_path = os.path.join(DATA_PATH, 'raw_html', date)

        for filename in os.listdir(html_path):

            # Open raw html content
            with open(os.path.join(html_path, filename), mode='r', encoding='ISO-8859-1') as file:
                html_file = file.read()

            # Get just the data part
            soup = BeautifulSoup(html_file, 'html.parser')
            html_data = soup.find(id='containerkpe_cont_kpeperfi')
            if not html_data:
                continue

            # Construct a PA dict with parsed data
            pa_d = {}
            filename_parser(filename, pa_d, pa_dict)
            parsers = [
                title_nif_parser, organismo_parser, ambito_parser, nuts_parser,
                web_oficial_parser, name_nif_parser, contacto_parser, pub_date_parser,
                tipo_poder_parser, act_principal_parser,
            ]

            for func in parsers:
                func(html_data, pa_d)

            cfile.write(json.dumps(pa_d, ensure_ascii=False) + '\n')


def title_nif_parser(soup, pa_d):
    """ Adds (`title`, `nif`) key(s) to PA dict """
    container = soup.find("div", "r01gCabeceraTitle")
    if container.h1:
        pa_d["pa_title"] = container.h1.string
    elif container.h3:
        pa_d["pa_title"], pa_d["pa_nif"] = container.h3.string.rsplit(' - ', 1)


def organismo_parser(soup, pa_d):
    """ Adds (`organismo`) key(s) to PA dict """
    container = soup.find_all("div", class_="r01SeccionTitulo", string='Órganos')
    ei_list = []

    if pa_d["pa_file_ver"] == "v1":
        for c in container:
            # Look for the previous sibling with text
            entidad_impulsora = {}
            for ps in c.previous_siblings:
                if ps.text.replace('\n', ''):
                    entidad_impulsora["ei_name"] = ps.text.replace('\n', '')
                    break

            # Look for the next sibling with text
            for ns in c.next_siblings:
                if ns.text.replace('\n', ''):
                    for br in ns.find_all("br"):
                        br.replace_with("\n")
                    entidad_impulsora["ei_organos"] = list(filter(None, ns.text.split('\n')))
                    break

            ei_list.append(entidad_impulsora)

    elif pa_d["pa_file_ver"] == "v2":
        for c in container:
            entidad_impulsora = {}
            # Look for the current parent previous sibling
            for pps in c.parent.previous_siblings:
                if pps.text.replace('\n', ''):
                    entidad_impulsora["ei_name"] = pps.text.replace('\n', '')
                    break

            # Look for the current parent divs that match class `r01clearfix orglistado`
            organos = []
            for ns in c.parent.find_all(class_="r01clearfix orglistado"):
                organos.append(ns.text.replace('\n', ''))
            entidad_impulsora["ei_organos"] = organos

            ei_list.append(entidad_impulsora)

    if ei_list:
        pa_d["pa_eis"] = ei_list


def pub_date_parser(soup, pa_d):
    """ Adds (`fecha_publicacion`) key(s) to PA dict """
    container = soup.find(string="Fecha de publicación")

    date = container.parent.parent.find("div", class_="r01SeccionTexto").string. \
        split(' ')[0].replace('ES210', 'ES21')

    pa_d["pa_fecha_publicacion"] = '/'.join(reversed(date.split('/')))


def ambito_parser(soup, pa_d):
    """ Adds (`ambito`) key(s) to PA dict """
    container = soup.find(string="Ámbito")
    pa_d["pa_ambito"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def nuts_parser(soup, pa_d: dict):
    """ Adds (`nuts`) key(s) to PA dict """
    container = soup.find(string="Código NUTS")
    if not container:
        return
    pa_d["pa_nuts"] = container.parent.parent.find("div", class_="r01SeccionTexto").string \
        .split(' ')[0].replace('ES210', 'ES21')


def act_principal_parser(soup, pa_d: dict):
    """ Adds (`act_principal`) key(s) to PA dict """
    container = soup.find(string="Principal actividad")

    if not container:
        container = soup.find(string="Actividad principal")

    if not container:
        return

    pa_d["pa_act_principal"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def tipo_poder_parser(soup, pa_d: dict):
    """ Adds (`tipo_poder`) key(s) to PA dict """
    container = soup.find(string="Tipo de poder")

    if not container:
        return

    pa_d["pa_tipo_poder"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def contacto_parser(soup, pa_d: dict):
    """ Adds (`contact`) key(s) to PA dict """
    container = soup.find(string="Contacto")
    if not container:
        return
    pa_d["pa_contact"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def web_oficial_parser(soup, pa_d: dict):
    """ Adds (`web`) key(s) to PA dict """
    container = soup.find(string="Web oficial")
    if not container:
        return
    web = container.parent.parent.find("a")["href"]
    empty_value_list = [
        "contratacion.eus",
        "contratacion.euscadi.eus",
        "contratacion.euskedi.eus",
        "contrataci?n.euskadi.eus",
    ]
    if not any(val in web for val in empty_value_list):
        pa_d["pa_web"] = web


def name_nif_parser(soup, pa_d: dict):
    """ Adds (`descrp`, `nif`) key(s) to PA dict """
    container = soup.find(string="Descripción")

    if not container or container.find_parents(class_="r01clearfix orgrecurso"):
        return

    arg1, arg2 = container.parent.parent.find("div", class_="r01SeccionTexto").string \
        .split(' - ', 1)

    if len(arg1) == 9:
        pa_d["pa_nif"], pa_d["pa_descrp"] = arg1, arg2
    else:
        pa_d["pa_nif"], pa_d["pa_descrp"] = arg2, arg1


def filename_parser(filename: str, pa_d: dict, pas_dict: dict):
    """ Adds (`file_html`, `cod_perfil`, `id_poder`, `name_es`, `name_eu`) key(s) to PA dict """
    filename = filename.removesuffix(".html")
    version, pa_cod_perfil = filename.split('_')

    if version == "v1":
        pa_d["pa_file_html"] = PA_DETAIL_URL_V1.format(codPerfil=pa_cod_perfil)
        pa_d["pa_file_ver"] = "v1"
    elif version == "v2":
        pa_d["pa_file_html"] = PA_DETAIL_URL_V2.format(codPerfil=pa_cod_perfil)
        pa_d["pa_file_ver"] = "v2"

    pa_dict = pas_dict[pa_cod_perfil]
    pa_d["pa_cod_perfil"] = pa_cod_perfil
    pa_d["pa_id_poder"] = pa_dict["idPoder"]
    pa_d["pa_name_es"] = pa_dict["nombreCortoEs"]  # CortoEs == LargoEs
    pa_d["pa_name_eu"] = pa_dict["nombreCortoEu"]  # CortoEu == LargoEu


if __name__ == "__main__":
    # get_pa_detail()
    parse_htmls(date='20220226', pa_dict=get_pa_dict())
