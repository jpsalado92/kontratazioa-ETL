"""
Functions for fetching and storing data related to bidders and classified bidders
from `www.contratación.euskadi.eus`
"""

import json
import os
from datetime import datetime

import requests

from e_utils import download_url_content
from scripts.transformers.t_bidders import get_cbidders_file
from scripts.transformers.t_utils import del_none
from scripts.utils import log

SCOPE = 'bidders'
TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
BIDDER_URL = "https://www.contratacion.euskadi.eus/ac70cPublicidadWar/busquedaAnuncios/autocompleteAdjudicatarios?q="
CBIDDER_URL = "https://www.contratacion.euskadi.eus/w32-kpesimpc/es/ac71aBusquedaRegistrosWar/empresas/filter"
CBIDDER_DETAIL_URL = "https://www.contratacion.euskadi.eus/ac71aBusquedaRegistrosWar/empresas/find"


def get_bidders():
    """
    Stores a jsonl file including a list of bidder entities
    as in contratacion.euskadi.eus
    Note: Not used as gives little information about bidders.
    """
    bidder_json = requests.get(BIDDER_URL).json()
    # Manage local directory path
    filename = os.path.join(DATA_PATH, SCOPE + '.jsonl')
    with open(filename, 'w', encoding='utf8') as file:
        for bidder in bidder_json:
            file.write(json.dumps(del_none(bidder), ensure_ascii=False) + '\n')


def get_bidders_from_conts():
    bidders_d = {}
    with open(os.path.join(DATA_PATH, '..', 'conts', 'conts.jsonl'), encoding='utf8') as conts_jsonl:
        for doc in conts_jsonl:
            doc_d = json.loads(doc)
            cif = doc_d['bidder_cif']
            name = doc_d['bidder_name']
            if not bidders_d.get('bidder_cif'):
                bidders_d[cif] = name
            else:
                if bidders_d[cif] != name:
                    raise ValueError(f"Given pkey CIF has multiple names {bidders_d[cif]} != {name}")


def get_classified_bidder_d():
    # Get classified bidders list
    get_rows = 10000
    payload = json.dumps({"rows": get_rows})
    bidder_d = requests.post(CBIDDER_URL, data=payload).json()
    if int(bidder_d["records"]) > get_rows:
        raise BrokenPipeError("More CBIDDER records than asked for!!")
    return bidder_d


def get_raw_cbidders_jsons(path):
    bidder_d = get_classified_bidder_d()
    # Prepare directory for bidders
    raw_dir = os.path.join(path, "raw_cbidders_jsons")
    os.makedirs(raw_dir, exist_ok=True)
    # Preparare list of request parameters and store location list
    rqfpath_list = []
    for bidder_c in bidder_d["rows"]:
        fpath = os.path.join(raw_dir, f"{bidder_c['cif']}.json")
        request_kwargs = {
            'url': CBIDDER_DETAIL_URL,
            'method': 'POST',
            'data': json.dumps({"nEmp": bidder_c["nEmp"]}),
            'headers': {'Content-Type': 'application/json'}
        }
        rqfpath_list.append((request_kwargs, fpath))
    download_url_content(rqfpath_list)


def get_detailed_cbidders(path):
    get_raw_cbidders_jsons(path)
    get_cbidders_file(path)


@log.start_end
def get_bidders(path):
    os.makedirs(path, exist_ok=True)
    get_detailed_cbidders(path)
    # get_bidders_from_conts()


if __name__ == "__main__":
    get_bidders(DATA_PATH)
