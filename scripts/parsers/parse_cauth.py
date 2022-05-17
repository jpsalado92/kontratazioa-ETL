"""
Functions for parsing data related to `cauth` (contracting authority) objects
from `www.contratación.euskadi.eus`

There are many fields that are not parsed from the raw_html files such as:
    CPV, Medio propio, Poderes adjudicadores, IICS, Órgano de recurso.
"""
import json
import logging

import os

from bs4 import BeautifulSoup


def parse_htmls(data_path: os.PathLike, cauth_dict: dict):
    """
    Based on raw html data, generates a cauth
    consolidated jsonl file at DATA_PATH

    :param data_path: Date used to indicate the location of raw html data. '20220225' like format
    :param cauth_dict: Dict built with `get_cauth_dict()`
    """
    time_stamp = os.path.basename(data_path)
    cfilename = os.path.join(data_path, '..', '..', '_'.join((time_stamp, 'cauth.jsonl')))
    with open(cfilename, mode='w', encoding='utf8') as cfile:
        for filename in os.listdir(data_path):

            # Open raw html content
            with open(os.path.join(data_path, filename), mode='r', encoding='ISO-8859-1') as file:
                html_file = file.read()

            # Get just the data part
            soup = BeautifulSoup(html_file, 'html.parser')
            html_data = soup.find(id='containerkpe_cont_kpeperfi')
            if not html_data:
                logging.info(f"No data in {filename}")
                print(f"No data in {filename}")
                continue

            # Construct a cauth dict with parsed data
            cauth_d = {}
            filename_parser(filename, cauth_d, cauth_dict)
            parsers = [
                title_nif_parser,
                organismo_parser,
                ambito_parser,
                nuts_parser,
                web_oficial_parser,
                name_nif_parser,
                contacto_parser,
                pub_date_parser,
                tipo_poder_parser,
                act_principal_parser,
            ]

            for func in parsers:
                func(html_data, cauth_d)

            cfile.write(json.dumps(cauth_d, ensure_ascii=False) + '\n')


def title_nif_parser(soup, cauth_d):
    """ Adds (`title`, `nif`) key(s) to cauth dict """
    container = soup.find("div", "r01gCabeceraTitle")
    if container.h1:
        cauth_d["cauth_title"] = container.h1.string
    elif container.h3:
        cauth_d["cauth_title"], cauth_d["cauth_nif"] = container.h3.string.rsplit(' - ', 1)


def organismo_parser(soup, cauth_d):
    """ Adds (`organismo`) key(s) to cauth dict """
    container = soup.find_all("div", class_="r01SeccionTitulo", string='Órganos')
    ei_list = []

    if cauth_d["cauth_file_ver"] == "v1":
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

    elif cauth_d["cauth_file_ver"] == "v2":
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
        cauth_d["cauth_eis"] = ei_list


def pub_date_parser(soup, cauth_d):
    """ Adds (`fecha_publicacion`) key(s) to cauth dict """
    container = soup.find(string="Fecha de publicación")

    date = container.parent.parent.find("div", class_="r01SeccionTexto").string. \
        split(' ')[0].replace('ES210', 'ES21')

    cauth_d["cauth_fecha_publicacion"] = '/'.join(reversed(date.split('/')))


def ambito_parser(soup, cauth_d):
    """ Adds (`ambito`) key(s) to cauth dict """
    container = soup.find(string="Ámbito")
    cauth_d["cauth_ambito"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def nuts_parser(soup, cauth_d: dict):
    """ Adds (`nuts`) key(s) to cauth dict """
    container = soup.find(string="Código NUTS")
    if not container:
        return
    cauth_d["cauth_nuts"] = container.parent.parent.find("div", class_="r01SeccionTexto").string \
        .split(' ')[0].replace('ES210', 'ES21')


def act_principal_parser(soup, cauth_d: dict):
    """ Adds (`act_principal`) key(s) to cauth dict """
    container = soup.find(string="Principal actividad")

    if not container:
        container = soup.find(string="Actividad principal")

    if not container:
        return

    cauth_d["cauth_act_principal"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def tipo_poder_parser(soup, cauth_d: dict):
    """ Adds (`tipo_poder`) key(s) to cauth dict """
    container = soup.find(string="Tipo de poder")

    if not container:
        return

    cauth_d["cauth_tipo_poder"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def contacto_parser(soup, cauth_d: dict):
    """ Adds (`contact`) key(s) to cauth dict """
    container = soup.find(string="Contacto")
    if not container:
        return
    cauth_d["cauth_contact"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def web_oficial_parser(soup, cauth_d: dict):
    """ Adds (`web`) key(s) to cauth dict """
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
        cauth_d["cauth_web"] = web


def name_nif_parser(soup, cauth_d: dict):
    """ Adds (`descrp`, `nif`) key(s) to cauth dict """
    container = soup.find(string="Descripción")

    if not container or container.find_parents(class_="r01clearfix orgrecurso"):
        return

    arg1, arg2 = container.parent.parent.find("div", class_="r01SeccionTexto").string \
        .split(' - ', 1)

    if len(arg1) == 9:
        cauth_d["cauth_nif"], cauth_d["cauth_descrp"] = arg1, arg2
    else:
        cauth_d["cauth_nif"], cauth_d["cauth_descrp"] = arg2, arg1


def filename_parser(filename: str, cauth_d: dict, cauths_dict: dict):
    """ Adds (`file_html`, `cod_perfil`, `id_poder`, `name_es`, `name_eu`) key(s) to cauth dict """
    filename = filename.removesuffix(".html")
    version, cauth_cod_perfil = filename.split('_')

    if version == "v1":
        cauth_d["cauth_file_html"] = \
            "https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/" \
            "poder_adjudicador/poder{codPerfil}/es_doc/es_arch_poder{codPerfil}.html" \
                .format(codPerfil=cauth_cod_perfil)
        cauth_d["cauth_file_ver"] = "v1"

    elif version == "v2":
        cauth_d["cauth_file_html"] = \
            "https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/" \
            "poder_adjudicador/poder{codPerfil}/es_doc/index.html" \
                .format(codPerfil=cauth_cod_perfil)
        cauth_d["cauth_file_ver"] = "v2"

    cauth_dict = cauths_dict[cauth_cod_perfil]
    cauth_d["cauth_cod_perfil"] = cauth_cod_perfil
    cauth_d["cauth_id_poder"] = cauth_dict["idPoder"]
    cauth_d["cauth_name_es"] = cauth_dict["nombreCortoEs"]  # CortoEs == LargoEs
    cauth_d["cauth_name_eu"] = cauth_dict["nombreCortoEu"]  # CortoEu == LargoEu
