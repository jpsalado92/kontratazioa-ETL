"""
Functions for fetching and storing data related to `ADJTs` (adjudicatarios) &
ADJT_Cs (adjudicatarios clasificados) from `www.contratación.euskadi.eus`

Useful resources:

    · ADJT_C register:
    https://www.contratacion.euskadi.eus/informacion-general-registro-licitadores-empresas-clasificadas/w32-kperoc/es/

    · ADJT_C finder:
    https://www.contratacion.euskadi.eus/w32-kperoc/es/ac71aBusquedaRegistrosWar/empresas/busqueda
"""

import json
import os
from datetime import datetime

import requests

from utils import del_none, retry

SCOPE = 'adjudicatarios'
DATA_PATH = os.path.join(os.getcwd(), '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.contratacion.euskadi.eus/"
ADJT_URL = BASE_URL + "ac70cPublicidadWar/busquedaAnuncios/autocompleteAdjudicatarios?q="
ADJT_C_URL = BASE_URL + "w32-kpesimpc/es/ac71aBusquedaRegistrosWar/empresas/filter"
ADJT_C_DETAIL_URL = BASE_URL + "ac71aBusquedaRegistrosWar/empresas/find"


def get_adjt():
    """
    Stores a jsonl file including normalized ADJT entities (dicts) as in contratacion.euskadi.eus
    """
    adjt_json = requests.get(ADJT_URL).json()
    # Manage local directory path
    filename = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE + '.jsonl')))
    with open(filename, 'w', encoding='utf8') as file:
        for adjt in adjt_json:
            adjt["name"] = adjt["razon"]
            # Get rid of empty keys that have always 0 value
            del adjt['identifiadorLicitadorWS']
            del adjt['razon']
            del adjt['uteString']
            file.write(json.dumps(del_none(adjt), ensure_ascii=False) + '\n')


def get_adjt_c():
    """
    Stores a jsonl file including normalized ADJT entities (dicts)
    as in contratacion.euskadi.eus
    """
    get_rows = 10000
    payload = json.dumps({"rows": get_rows})
    r_json = requests.post(ADJT_C_URL, data=payload).json()

    if int(r_json["records"]) > get_rows:
        print("More ADJT_C records than asked for!!")

    cfilename = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE, 'clasificados.jsonl')))
    with open(cfilename, 'w', encoding='utf8') as cfile:
        for adjt_c in r_json["rows"]:
            adjt_c_detail = get_adjt_c_detail(adjt_c["nEmp"])
            adjt_c_norm = normalize_adjt_c(adjt_c_detail)
            cfile.write(json.dumps(adjt_c_norm, ensure_ascii=False) + '\n')


@retry(3, [requests.exceptions.ConnectionError, ])
def get_adjt_c_detail(n_emp: str):
    """ Fetch details from the ADJT_C specified by `nEmp` """
    payload = json.dumps({"nEmp": n_emp})
    headers = {'Content-Type': 'application/json'}
    r_json = requests.post(ADJT_C_DETAIL_URL, headers=headers, data=payload).json()
    return del_none(r_json)


def normalize_adjt_c(adjt_d: dict) -> dict:
    """ Returns a dict containing a normalized ADJT entity """
    return {
        "cif": adjt_d["cif"],
        "name": adjt_d["denominacionSocialNorm"],
        "location_province": adjt_d.get("provinciaDesCas"),
        "location_municipalty": adjt_d.get("municipioDes"),
        "address": adjt_d.get("direccion"),
        "purpose": adjt_d.get("objeto"),
        "tipoact_list": get_tipoact_list(adjt_d),
        "obras_list": get_obr_list(adjt_d),
        "servicios_list": get_ser_list(adjt_d),
        "is_adjt_c": True,
    }


def get_tipoact_list(adjt_d):
    """ Returns the list of `tipoact` available for a given ADJT """
    return [act["codTipoActividad"] for act in adjt_d["listaActEconomicas"]]


def get_obr_list(adjt_d):
    """ Returns the list of `obras` available for a given ADJT """
    obr_est_list = get_serobr_children(adjt_d, 'listaObrasEstatal')
    obr_aut_list = get_serobr_children(adjt_d, 'listaObrasAutonomico')
    return list(set(obr_est_list + obr_aut_list))


def get_ser_list(adjt_d):
    """ Returns the list of `servicios` available for a given ADJT """
    ser_est_list = get_serobr_children(adjt_d, 'listaServiciosEstatal')
    ser_aut_list = get_serobr_children(adjt_d, 'listaServiciosAutonomico')
    return list(set(ser_est_list + ser_aut_list))


def get_serobr_children(adjt_d: dict, key: str):
    """ Flattens and returns a list containing Returns the list of `servicios` available for a given ADJT """
    try:
        return ['_'.join((item["grupo"], item["subgrupo"], item["cate"])) for item in adjt_d.get(key)]
    except KeyError:
        return ['_'.join((item["grupo"], item["subgrupo"], "Z")) for item in adjt_d.get(key)]
    except TypeError:
        return []


if __name__ == "__main__":
    get_adjt()
    get_adjt_c()
