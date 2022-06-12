"""
Functions for normalizing data related to bidder objects
from `www.contratación.euskadi.eus`
"""
import json
import os


def get_cbidders_file(path):
    """ Parses and cleans cbidder `json` data and stores it in a cbidders `.jsonl` file """
    cfilename = os.path.join(path, 'cbidders.jsonl')
    with open(cfilename, 'w', encoding='utf8') as cfile:
        raw_cbidders_path = os.path.join(path, "raw_cbidders_jsons")
        # Iterating through every CBIDDER json
        for json_fname in os.listdir(raw_cbidders_path):
            try:
                with open(os.path.join(raw_cbidders_path, json_fname), 'r', encoding='utf8') as jsonf:
                    cbidder_d = json.load(jsonf)
            except json.JSONDecodeError:
                print(json_fname)
            # bidder_c_norm = norm_cbidder(cbidder_d)
            cfile.write(json.dumps(cbidder_d, ensure_ascii=False) + '\n')


def norm_cbidder(bidder_d: dict) -> dict:
    """ Returns a dict containing a normalized bidder entity """
    return {
        "cif": bidder_d["cif"],
        "name": bidder_d["denominacionSocialNorm"],
        "location_province": bidder_d.get("provinciaDesCas"),
        "location_municipalty": bidder_d.get("municipioDes"),
        "address": bidder_d.get("direccion"),
        "purpose": bidder_d.get("objeto"),
        "tipoact_list": get_cbidder_tipoact_list(bidder_d),
        "obras_list": get_cbidder_obr_list(bidder_d),
        "servicios_list": get_cbidder_ser_list(bidder_d),
        "is_bidder_c": True,
    }


def get_cbidder_tipoact_list(bidder_d):
    """ Returns the list of `tipoact` available for a given cbidder """
    return [act["codTipoActividad"] for act in bidder_d["listaActEconomicas"]]


def get_cbidder_obr_list(bidder_d):
    """ Returns the list of `obras` available for a given cbidder """
    obr_est_list = get_serobr_children(bidder_d, 'listaObrasEstatal')
    obr_aut_list = get_serobr_children(bidder_d, 'listaObrasAutonomico')
    return list(set(obr_est_list + obr_aut_list))


def get_cbidder_ser_list(bidder_d):
    """ Returns the list of `servicios` available for a given cbidder """
    ser_est_list = get_serobr_children(bidder_d, 'listaServiciosEstatal')
    ser_aut_list = get_serobr_children(bidder_d, 'listaServiciosAutonomico')
    return list(set(ser_est_list + ser_aut_list))


def get_serobr_children(bidder_d: dict, key: str):
    """ Flattens and returns the list of `servicios` available for a given ADJT """
    try:
        return ['_'.join((item["grupo"], item["subgrupo"], item["cate"])) for item in bidder_d.get(key)]
    except KeyError:
        return ['_'.join((item["grupo"], item["subgrupo"], "Z")) for item in bidder_d.get(key)]
    except TypeError:
        return []
