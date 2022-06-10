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

from scripts.utils import log


@log.start_end
def get_cauths_file(path: os.PathLike, cauths_dict):
    """
    Based on raw html data, generates a cauth
    consolidated jsonl file at DATA_PATH
    """
    cfilename = os.path.join(path, 'cauths.jsonl')
    with open(cfilename, mode='w', encoding='utf8') as cfile:
        raw_data_path = os.path.join(path, 'raw_html')
        for filename in os.listdir(raw_data_path):
            # Open raw html content
            with open(os.path.join(raw_data_path, filename), mode='r', encoding='ISO-8859-1') as file:
                html_file = file.read()
            # Get just the data part
            soup = BeautifulSoup(html_file, 'html.parser')
            html_data = soup.find(id='containerkpe_cont_kpeperfi')
            if not html_data:
                logging.info(f"No data in {filename}")
                continue
            # Construct a cauth dict with parsed data
            cauth_d = {}
            parse_filename(filename, cauth_d, cauths_dict)
            parsers = [
                parse_url_official,
                parse_url_logo,
                parse_date_published,
                parse_title_nif,
                parse_name_nif,
                parse_location_nuts,
                parse_location_address,
                parse_type_authority,
                parse_type_main_activity,
                parse_list_promoters,
                # parse_location_ambito,
            ]
            for func in parsers:
                func(html_data, cauth_d)
            cfile.write(json.dumps(cauth_d, ensure_ascii=False) + '\n')


def parse_title_nif(soup, cauth_d):
    """ Adds (`title`, `nif`) key(s) to cauth dict """
    container = soup.find("div", "r01gCabeceraTitle")
    if container.h1:
        # cauth_d["title"] = container.h1.string
        pass
    elif container.h3:
        # cauth_d["title"], cauth_d["nif"] = container.h3.string.rsplit(' - ', 1)
        _, cauth_d["nif"] = container.h3.string.rsplit(' - ', 1)


def parse_name_nif(soup, cauth_d: dict):
    """ Adds (`descrp`, `nif`) key(s) to cauth dict """
    container = soup.find(string="Descripción")
    if not container or container.find_parents(class_="r01clearfix orgrecurso"):
        return
    arg1, arg2 = container.parent.parent.find("div", class_="r01SeccionTexto").string.split(' - ', 1)
    if len(arg1) == 9:
        # cauth_d["nif"], cauth_d["descrp"] = arg1, arg2
        cauth_d["nif"], _ = arg1, arg2
    else:
        # cauth_d["nif"], cauth_d["descrp"] = arg2, arg1
        cauth_d["nif"], _ = arg2, arg1


def parse_location_nuts(soup, cauth_d: dict):
    """ Adds (`location_nuts`) key(s) to cauth dict """
    container = soup.find(string="Código NUTS")
    if not container:
        return
    cauth_d["location_nuts"] = container.parent.parent.find("div", class_="r01SeccionTexto").string \
        .split(' ')[0].replace('ES210', 'ES21')


def parse_location_address(soup, cauth_d: dict):
    """ Adds (`location_address`) key(s) to cauth dict """
    container = soup.find(string="Contacto")
    if not container:
        return
    cauth_d["location_address"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def parse_url_official(soup, cauth_d: dict):
    """ Adds (`url_official`) key(s) to cauth dict """
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
        cauth_d["url_official"] = web


def parse_list_promoters(soup, cauth_d):
    """ Adds (`organismo`) key(s) to cauth dict """
    container = soup.find_all("div", class_="r01SeccionTitulo", string='Órganos')
    list_promoters = []

    if cauth_d["cauth_version"] == "v1":
        for c in container:
            # Look for the previous sibling with text
            entidad_impulsora = {}
            for ps in c.previous_siblings:
                if ps.text.replace('\n', ''):
                    entidad_impulsora["promoter_name"] = ps.text.replace('\n', '')
                    break
            # Look for the next sibling with text
            for ns in c.next_siblings:
                if ns.text.replace('\n', ''):
                    for br in ns.find_all("br"):
                        br.replace_with("\n")
                    entidad_impulsora["list_organism"] = list(filter(None, ns.text.split('\n')))
                    break
            list_promoters.append(entidad_impulsora)

    elif cauth_d["cauth_version"] == "v2":
        for c in container:
            entidad_impulsora = {}
            # Look for the current parent previous sibling
            for pps in c.parent.previous_siblings:
                if pps.text.replace('\n', ''):
                    entidad_impulsora["promoter_name"] = pps.text.replace('\n', '')
                    break
            # Look for the current parent divs that match class `r01clearfix orglistado`
            list_organism = []
            for ns in c.parent.find_all(class_="r01clearfix orglistado"):
                list_organism.append(ns.text.replace('\n', ''))
            entidad_impulsora["list_organism"] = list_organism
            list_promoters.append(entidad_impulsora)

    if list_promoters:
        cauth_d["list_promoters"] = list_promoters


def parse_date_published(soup, cauth_d):
    """ Adds (`date_published`) key(s) to cauth dict """
    container = soup.find(string="Fecha de publicación")
    date = container.parent.parent.find("div", class_="r01SeccionTexto").string. \
        split(' ')[0].replace('ES210', 'ES21')
    cauth_d["date_published"] = '/'.join(reversed(date.split('/')))


ALIAS_TYPE_MAIN_ACTIVITY = {
    'TICs': 'TICs',
    'Promoción de la igualdad': 'Promoción de la Igualdad',
    'Innovación': 'Ciencia, Innovación e Investigación',
    'Ocio, cultura y religión': 'Ocio, Cultura y Religión',
    'Vivienda y servicios comunitarios': 'Vivienda y Servicios Comunitarios',
    'Educación': 'Educación',
    'Parlamento': 'Parlamento',
    'Otra actividad': 'OTROS',
    'Asuntos económicos y financieros': 'Asuntos Económicos y Financieros',
    'Servicios Informáticos': 'TICs',
    'TIC': 'TICs',
    'Investigación': 'Ciencia, Innovación e Investigación',
    'Transportes': 'Transportes',
    'Promoción Económica': 'Asuntos Económicos y Financieros',
    'Promoción empresarial': 'Promoción Empresarial',
    'Telecomunicaciones': 'TICs',
    'Servicios públicos generales': 'Vivienda y Servicios Comunitarios',
    'Obras de urbanización': 'Vivienda y Servicios Comunitarios',
    'Ganadería': 'Alimentación',
    'Turismo': 'Turismo',
    'Promoción Empresarial': 'Promoción Empresarial',
    'Medio ambiente': 'Medioambiente',
    'Protección social': 'Servicios Sociales',
    'Ciencia e innovación': 'Ciencia, Innovación e Investigación',
    'Explotación de una central eléctrica': 'Producción de Energía Eléctrica',
    'Ganaderia': 'Alimentación',
    'Alimentación': 'Alimentación',
    'Salud': 'Salud',
    'Ikerketa': 'Ciencia, Innovación e Investigación',
    'Servicios Sociales': 'Servicios Sociales',
    'Administración y uso sostenible de los montes': 'Medioambiente',
    'Producción de energía eléctrica': 'Producción de Energía Eléctrica'
}


def parse_type_main_activity(soup, cauth_d: dict):
    """ Adds (`type_main_activity`) key(s) to cauth dict """
    container = soup.find(string="Principal actividad")
    if not container:
        container = soup.find(string="Actividad principal")
    if not container:
        return
    alias = container.parent.parent.find("div", class_="r01SeccionTexto").string.replace('\n', '')
    try:
        cauth_d["type_main_activity"] = ALIAS_TYPE_MAIN_ACTIVITY[alias]
    except KeyError:
        logging.warning(f"No value for alias {alias}")
        cauth_d["type_main_activity"] = 'OTROS'


def parse_type_authority(soup, cauth_d: dict):
    """ Adds (`type_authority`) key(s) to cauth dict """
    container = soup.find(string="Tipo de poder")
    if not container:
        return
    cauth_d["type_authority"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def parse_filename(filename: str, cauth_d: dict, cauths_dict: dict):
    """ Adds (`cod_perfil`, `name`, `cauth_version`, `url_kontratazioa`) key(s) to cauth dict """
    filename = filename.removesuffix(".html")
    version, cod_perfil = filename.split('_')
    cauth_d["cod_perfil"] = cod_perfil
    cauth_d["name"] = cauths_dict[cod_perfil]["nombreCortoEs"]
    if version == "v1":
        cauth_d["cauth_version"] = "v1"
        cauth_d["url_kontratazioa"] = \
            "https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/" \
            "poder_adjudicador/poder{codPerfil}/es_doc/es_arch_poder{codPerfil}.html" \
                .format(codPerfil=cod_perfil)
    elif version == "v2":
        cauth_d["cauth_version"] = "v2"
        cauth_d["url_kontratazioa"] = \
            "https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/" \
            "poder_adjudicador/poder{codPerfil}/es_doc/index.html" \
                .format(codPerfil=cod_perfil)


def parse_location_ambito(soup, cauth_d):
    """ Adds (`location_ambito`) key(s) to cauth dict """
    container = soup.find(string="Ámbito")
    cauth_d["location_ambito"] = container.parent.parent.find("div", class_="r01SeccionTexto").string


def parse_url_logo(soup, cauth_d):
    """ Adds (`url_logo`) key(s) to cauth dict """
    try:
        cauth_d["url_logo"] = 'https://www.contratacion.euskadi.eus/' + soup.find('img', alt="Poder adjudicador")['src']
    except TypeError:
        logging.info(f"No logo available for {cauth_d['cod_perfil']}")
