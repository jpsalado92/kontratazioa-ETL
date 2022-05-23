"""
Functions for fetching and storing dimensions-data related to tenders
from `www.contratación.euskadi.eus`
"""
import json
import os
from datetime import datetime

import requests

from scripts.utils.utils import del_none, strip_dict

TIME_STAMP = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.contratacion.euskadi.eus/"
NUTS_DIM_URL = BASE_URL + "ac70cPublicidadWar/busquedaAnuncios/autocompleteNuts?q="
CPV_DIM_URL = BASE_URL + "ac70cPublicidadWar/busquedaAnuncios/autocompleteCpv?q="
PAIS_DIM_URL = BASE_URL + "ac70cPublicidadWar/busquedaAnuncios/autocompletePaises?q="
TIPOACT_DIM_URL = BASE_URL + "ac71aBusquedaRegistrosWar/comboMaestros/getActividadesEconomicasLike?&q=&c=true"
CATEGORIA_DIM_URL = BASE_URL + "ac71aBusquedaRegistrosWar/comboMaestros/findCategoria"
SUBGRUPO_DIM_URL = BASE_URL + "ac71aBusquedaRegistrosWar/comboMaestros/findSubgrupoCategoria"
GRUPO_DIM_URL = BASE_URL + "ac71aBusquedaRegistrosWar/comboMaestros/findGrupoCategoria"

SCOPE = 'dimensions'

DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', SCOPE)


def get_nuts_dim():
    """ Fetches and stores `nuts` dimension """
    nuts_list = requests.get(NUTS_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'nuts_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for nuts_d in nuts_list:
            file.write(json.dumps(del_none(nuts_d['nuts']), ensure_ascii=False) + '\n')


def get_cpv_dim():
    """ Fetches and stores `cpv` dimension """
    cpv_list = requests.get(CPV_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'cpv_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for cpv_d in cpv_list:
            # Get rid of empty data
            del cpv_d['cpvHijos']
            del cpv_d['principalString']
            file.write(json.dumps(del_none(cpv_d), ensure_ascii=False) + '\n')


def get_pais_dim():
    """ Fetches and stores `pais` dimension """
    pais_list = requests.get(PAIS_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'pais_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for pais_d in pais_list:
            # Get rid of empty data
            del pais_d['pais']['estado']
            file.write(json.dumps(del_none(pais_d['pais']), ensure_ascii=False) + '\n')


def get_tipoact_dim():
    """ Fetches and stores `tipoact` dimension """
    tipoact_list = requests.get(TIPOACT_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'tipoact_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for tipoact_d in tipoact_list:
            # Get rid of empty data
            del tipoact_d['tipoActicidadEconomica']
            tipoact_d['descTipoActividad'] = tipoact_d['descTipoActividad'].strip()
            file.write(json.dumps(del_none(tipoact_d), ensure_ascii=False) + '\n')


def get_categoria_dim():
    """ Fetches and stores `categoria` dimension """
    categoria_list = requests.get(CATEGORIA_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'categoria_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for categoria_d in categoria_list:
            strip_dict(categoria_d)
            file.write(json.dumps(del_none(categoria_d), ensure_ascii=False) + '\n')


def get_subgrupo_dim():
    """ Fetches and stores `subgrupo` dimension """
    subgrupo_list = requests.get(SUBGRUPO_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'subgrupo_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for subgrupo_d in subgrupo_list:
            strip_dict(subgrupo_d)
            # Get rid of empty data
            del subgrupo_d['codPk']
            del subgrupo_d['codPkAfin']
            del subgrupo_d['grupo']
            file.write(json.dumps(del_none(subgrupo_d), ensure_ascii=False) + '\n')


def get_grupo_dim():
    """
    Fetches and stores `grupo` dimension
        codClas":"S" -> Servicio
        codClas":"O" -> Obras
    """
    grupo_list = requests.get(GRUPO_DIM_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, 'grupo_dimension.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for grupo_d in grupo_list:
            strip_dict(grupo_d)
            file.write(json.dumps(del_none(grupo_d), ensure_ascii=False) + '\n')


if __name__ == "__main__":
    os.makedirs(DATA_PATH, exist_ok=True)
    get_nuts_dim()
    get_cpv_dim()
    get_pais_dim()
    get_tipoact_dim()
    get_categoria_dim()
    get_subgrupo_dim()
    get_grupo_dim()
