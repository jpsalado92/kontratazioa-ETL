import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import requests

from get_pa import get_pa_dict_list
from utils import retry

SCOPE = "contratos"
DATA_PATH = os.path.join(os.getcwd(), '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
RAW_PA_CONTS_PATH = os.path.join(DATA_PATH, 'raw_pa_conts', TIME_STAMP)
BASE_URL = "https://www.contratacion.euskadi.eus/"
COOKIES_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                         "/busquedaInformesOpenData" \
                         "?locale=es"
COOKIES = requests.get(COOKIES_URL).headers['Set-Cookie']

CONT_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar/indicadorREST" \
                      "/descargarInformeOpenData" \
                      "?idioma=es" \
                      "&anio={report_date}" \
                      "&idPoder={codperfil}" \
                      "&R01HNoPortal=true"
CONT_BY_PA_LIST_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                                 "/busquedaInformesOpenData" \
                                 "/tablaInformes/filter"

XML_LIST_FIELDS = (
    'publicacionesBOTH', 'modificacionesIncidencia', 'publicacionesBOE',
    'publicacionesBOPV', 'clausulasEspeciales', 'anualidades', 'ivas', 'nutses',
    'prorrogas', 'modificacionesPlazo', 'modificacionesObjeto', 'finalizaciones',
    'lugaresAplicacion', 'resoluciones', 'publicacionesDOUE')


def get_raw_cont_files():
    """ Fetch and store `xml` and `json` CONT data locally """
    for pa_d in get_pa_dict_list():
        pa_cod_perfil = pa_d['codPerfil']

        for cont in get_cont_list_by_pa(pa_cod_perfil=pa_cod_perfil):
            cont_date = str(int(cont['anioInforme']))
            cont_mod_date = cont['fechaModif'].replace('-', '')
            cont_id = str(int(cont['idInformeOpendata']))
            cont_fname = f"{int(pa_cod_perfil):05d}_{cont_date}_{cont_id}_{cont_mod_date}"

            cont_fpath = os.path.join(RAW_PA_CONTS_PATH, cont_fname)
            if os.path.isdir(cont_fpath):
                continue

            os.makedirs(cont_fpath, exist_ok=True)
            json_fpath = os.path.join(cont_fpath, cont_fname + '.json')
            with open(json_fpath, mode='w', encoding='utf8') as file:
                del cont['poder']
                file.write(json.dumps(cont, indent=2, ensure_ascii=False))

            # Fetch, store and extract zip file
            zip_url = CONT_URL.format(codperfil=pa_cod_perfil, report_date=cont_date)
            try:
                r = requests.get(zip_url)
                xml_fpath = os.path.join(cont_fpath, cont_fname + '.xml')
                with ZipFile(BytesIO(r.content)) as zfile:
                    for file in zfile.namelist():
                        if file.endswith('.xml'):
                            zfile.extract(file, cont_fpath)
                            os.rename(os.path.join(cont_fpath, file.replace('"', '_')), xml_fpath)
            except BadZipFile:
                print(zip_url)


@retry(times=5, exceptions=json.decoder.JSONDecodeError, sleep=0.5)
def get_cont_list_by_pa(pa_cod_perfil):
    """ Fetch CONTs under a given PA as a list"""
    payload = \
        {
            "length": 1000000,
            "filter": {
                "poder": {"codPerfil": pa_cod_perfil},
                "anioDesde": "2000",
                "anioHasta": "2030"
            },
            "rows": 1000000,
            "page": 1
        }
    r_json = requests.post(CONT_BY_PA_LIST_URL,
                           headers={'Cookie': COOKIES},
                           data=json.dumps(payload),
                           timeout=25).json()

    if int(r_json['page']) > 1:
        print("More data available than expected!")
        raise

    return r_json["rows"]


def get_cont_file():
    with open(os.path.join(DATA_PATH, f"{TIME_STAMP}_contratos.jsonl"), 'w') as contfile:
        # Iterating through every CONT directory
        for dirname in os.listdir(RAW_PA_CONTS_PATH):
            dir_path = os.path.join(RAW_PA_CONTS_PATH, dirname)
            pa_cod_perfil, cont_date, cont_id, cont_mod_date = dirname.split('_')
            pa_cod_perfil = str(int(pa_cod_perfil))
            cont_d = {"cont_date": cont_date, "pa_cod_perfil": pa_cod_perfil,
                      "cont_id": cont_id, "cont_mod_date": cont_mod_date}
            # Iterating through every CONT in a given `xml` file
            if len(os.listdir(dir_path)) != 2:
                continue
            for xml_cont in ET.parse(os.path.join(dir_path, dirname + '.xml')).getroot():
                parsed_cont_d = parse_xml_field(node=xml_cont, dict_obj=cont_d.copy())
                parsed_cont_d["codExp"], *_ = parsed_cont_d["codContrato"].split('_')
                contfile.write(json.dumps(parsed_cont_d, ensure_ascii=False) + '\n')


def parse_xml_field(node, path='', dict_obj=None):
    if dict_obj is None:
        dict_obj = {}
    try:
        node_text = clean_xml_text(node.text)
    except:
        node_text = None

    node_tag = node.tag.replace('{com/ejie/ac70a/xml/opendata}', '').replace('contratoOpenData', '')
    if path:
        new_path = '-'.join((path, node_tag))
    else:
        new_path = node_tag

    if node_tag in XML_LIST_FIELDS:
        container = []
        for child in node:
            dd = {}
            parse_xml_field(child, '', dd)
            container.append(dd.copy())
        dict_obj[new_path] = container.copy()

    else:
        if node_text:
            if new_path in dict_obj:
                print(new_path)
                raise
            else:
                dict_obj[new_path] = node_text

        for child in node:
            parse_xml_field(child, new_path, dict_obj)

    return dict_obj


def clean_xml_text(text):
    # Clean line breaks and starting or ending spaces
    text = text.strip().replace('\n', '').strip()

    if not text:
        return None

    # Handle integers and numbers
    try:
        if '_' not in text:
            if '.' in text:
                return float(text)
            else:
                return int(text)
    except:
        pass

    # Handle boolean values
    if text == "FALSE":
        return False
    elif text == "TRUE":
        return True

    return text


if __name__ == "__main__":
    get_raw_cont_files()
    get_cont_file()
