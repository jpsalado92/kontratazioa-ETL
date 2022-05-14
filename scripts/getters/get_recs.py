"""
Functions for fetching and storing data related to `RECs` (recursos)
from `www.contratación.euskadi.eus`

Useful resources:

    · REC General information:
    https://www.contratacion.euskadi.eus/informacion-general-oarc/w32-kpeoarc/es/

    · REC guidelines:
    https://www.contratacion.euskadi.eus/normativa-oarc/w32-kpeoarc/es/

    · REC register:
    https://www.contratacion.euskadi.eus/w32-kpeoarc/es/y96aResolucionesWar/busqueda/listado?locale=es

    · Sample:
    https://www.contratacion.euskadi.eus/w32-kpeoarc/es/contenidos/resolucion_oarc/3_2015/es_def/index.shtml
TODO: FORMAT OUTPUTS
"""


import json
import os
from datetime import datetime

import requests

from utils import del_none

SCOPE = 'recursos'
DATA_PATH = os.path.join(os.getcwd(), '../..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.contratacion.euskadi.eus/"
REC_URL = BASE_URL + "y96aResolucionesWar/busqueda/buscarListado?R01HNoPortal=true"


def get_rec():
    """
    Fetches and stores a jsonl file including REC
    entities (dicts) as in contratacion.euskadi.eus
    """
    rec_list = requests.get(REC_URL).json()
    filepath = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE + '.jsonl')))
    with open(filepath, 'w', encoding='utf8') as file:
        for rec in rec_list:
            file.write(json.dumps(del_none(rec), ensure_ascii=False) + '\n')


if __name__ == "__main__":
    get_rec()
