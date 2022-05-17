"""
Functions for fetching and storing data related to bidders and classified bidders
from `www.contratación.euskadi.eus`
"""

import json
import os
from datetime import datetime

import requests
from scripts.normalizers.norm_bidder import norm_cbidder
from scripts.utils.utils import del_none, retry

SCOPE = 'bidders'
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")
BIDDER_URL = "https://www.contratacion.euskadi.eus/ac70cPublicidadWar/busquedaAnuncios/autocompleteAdjudicatarios?q="
CBIDDER_URL = "https://www.contratacion.euskadi.eus/w32-kpesimpc/es/ac71aBusquedaRegistrosWar/empresas/filter"
CBIDDER_DETAIL_URL = "https://www.contratacion.euskadi.eus/ac71aBusquedaRegistrosWar/empresas/find"


def get_bidders():
    """
    Stores a jsonl file including a list of bidder entities
    as in contratacion.euskadi.eus
    """
    bidder_json = requests.get(BIDDER_URL).json()
    # Manage local directory path
    filename = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE + '.jsonl')))
    with open(filename, 'w', encoding='utf8') as file:
        for bidder in bidder_json:
            file.write(json.dumps(del_none(bidder), ensure_ascii=False) + '\n')


def get_detailed_cbidders():
    """
    Stores a jsonl file including a list of classified bidder entities (dicts)
    as in contratacion.euskadi.eus
    """
    get_rows = 10000
    payload = json.dumps({"rows": get_rows})
    r_json = requests.post(CBIDDER_URL, data=payload).json()

    if int(r_json["records"]) > get_rows:
        print("More CBIDDER records than asked for!!")

    cfilename = os.path.join(DATA_PATH, '_'.join((TIME_STAMP, SCOPE, 'clasificados.jsonl')))
    with open(cfilename, 'w', encoding='utf8') as cfile:
        for bidder_c in r_json["rows"]:
            bidder_c_detail = get_cbidder_detail(bidder_c["nEmp"])
            bidder_c_norm = norm_cbidder(bidder_c_detail)
            cfile.write(json.dumps(bidder_c_norm, ensure_ascii=False) + '\n')


@retry(3, [requests.exceptions.ConnectionError, ])
def get_cbidder_detail(n_emp: str):
    """ Fetch details from the classified bidder specified by `nEmp` """
    payload = json.dumps({"nEmp": n_emp})
    headers = {'Content-Type': 'application/json'}
    r_json = requests.post(CBIDDER_DETAIL_URL, headers=headers, data=payload).json()
    return del_none(r_json)


if __name__ == "__main__":
    os.makedirs(DATA_PATH, exist_ok=True)
    get_bidders()
    get_detailed_cbidders()
