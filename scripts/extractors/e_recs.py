"""
Functions for fetching and storing data related to `rec` (appeals) objects
from `www.contratación.euskadi.eus`
"""

import json
import os
from datetime import datetime

import requests

from scripts.utils.utils import del_none

SCOPE = 'rec'
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
REC_URL = "https://www.contratacion.euskadi.eus/y96aResolucionesWar/busqueda/buscarListado?R01HNoPortal=true"


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
    os.makedirs(DATA_PATH, exist_ok=True)
    get_rec()
