"""
Functions for fetching and storing data related to bidders and classified bidders
"""

import json
import os
from datetime import datetime

import requests

from scripts.extractors.e_utils import async_download_urls
from scripts.transformers.t_bidders import get_cbidders_dict
from scripts.utils import log

SCOPE = 'bidders'
TIME_STAMP = datetime.now().strftime("%Y%m%d")
DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'data', TIME_STAMP, SCOPE)
BIDDERS_URL = "https://www.contratacion.euskadi.eus/ac70cPublicidadWar/busquedaAnuncios/autocompleteAdjudicatarios?q="
CBIDDERS_URL = "https://www.contratacion.euskadi.eus/w32-kpesimpc/es/ac71aBusquedaRegistrosWar/empresas/filter"
CBIDDER_DETAIL_URL = "https://www.contratacion.euskadi.eus/ac71aBusquedaRegistrosWar/empresas/find"


def get_bidders_from_conts(path):
    bidders_d = {}
    with open(os.path.join(path, '..', 'conts', 'conts.jsonl'), encoding='utf8') as conts_jsonl:
        for doc in conts_jsonl:
            doc_d = json.loads(doc)
            cif = doc_d['bidder_cif']
            name = doc_d['bidder_name']
            if not bidders_d.get('bidder_cif'):
                bidders_d[cif] = {'name': name}
            else:
                if bidders_d[cif] != name:
                    raise ValueError(f"Given pkey CIF has multiple names {bidders_d[cif]} != {name}")
    return bidders_d


def get_classified_bidder_d():
    get_rows = 10000
    payload = json.dumps({"rows": get_rows})
    cbidders_d = requests.post(CBIDDERS_URL, data=payload).json()
    if int(cbidders_d["records"]) > get_rows:
        raise BrokenPipeError("More CBIDDER records than asked for!!")
    return cbidders_d


def get_raw_cbidders_jsons(path):
    cbidders_d = get_classified_bidder_d()
    # Prepare directory for bidders
    raw_dir = os.path.join(path, "raw_cbidders_jsons")
    os.makedirs(raw_dir, exist_ok=True)
    # Preparare list of request parameters and store location list
    rqfpath_list = []
    for cbidder in cbidders_d["rows"]:
        fpath = os.path.join(raw_dir, f"{cbidder['cif']}.json")
        request_kwargs = {'url': CBIDDER_DETAIL_URL, 'method': 'POST', 'data': json.dumps({"nEmp": cbidder["nEmp"]}),
                          'headers': {'Content-Type': 'application/json'}}
        rqfpath_list.append((request_kwargs, fpath))
    async_download_urls(rqfpath_list)


def get_detailed_cbidders(path):
    get_raw_cbidders_jsons(path)
    return get_cbidders_dict(path)


@log.start_end
def get_bidders(path):
    os.makedirs(path, exist_ok=True)
    cbidders_d = get_detailed_cbidders(path)
    bidders_d = get_bidders_from_conts(path)
    full_bidders_d = dict(bidders_d, **cbidders_d)
    with open(os.path.join(path, 'bidders.jsonl'), 'w', encoding='utf8') as jsonl:
        for cif in full_bidders_d:
            to_file_d = {'cif': cif} | full_bidders_d[cif]
            jsonl.write(json.dumps(to_file_d, ensure_ascii=False) + '\n')


if __name__ == "__main__":
    get_bidders(DATA_PATH)
