"""
Functions for normalizing data related to bidder objects
from `www.contratación.euskadi.eus`
"""
import json
import logging
import os

from scripts.utils.utils import flatten


def get_cbidders_dict(path):
    """ Parses and cleans CBIDDER `json` data and stores it in a dict """
    cbidders_d = {}
    raw_cbidders_path = os.path.join(path, "raw_cbidders_jsons")
    # Iterating through every CBIDDER json
    for json_fname in os.listdir(raw_cbidders_path):
        with open(os.path.join(raw_cbidders_path, json_fname), 'r', encoding='utf8') as jsonf:
            b_dict = json.load(jsonf)
        cbidders_d[b_dict["cif"]] = {
            "name": b_dict.get("denominacionSocial"),
            "purpose": b_dict.get("objeto"),
            "location_nuts": parse_nuts(b_dict),
            "location_address": b_dict.get("direccion"),
            "location_municipalty": b_dict.get("municipioDes"),
            "list_iae": parse_list_cpv(b_dict),
            "list_serobr": parse_list_serobr(b_dict),
            "is_overdue_certificate": b_dict.get("certificadoCaducado"),
            "is_classified_bidder": True,
            # "n_emp": b_dict.get("nEmp"),
            # "n_insc": b_dict.get("nInsc"),
        }
    return cbidders_d


ALIAS_NUTS = {'La Coruña': 'ES111', 'Coruña, A': 'ES111', 'Lugo': 'ES112', 'Orense': 'ES113', 'Ourense': 'ES113',
              'Pontevedra': 'ES114', 'Principado de Asturias': 'ES120', 'Asturias': 'ES120', 'Cantabria': 'ES130',
              'Álava': 'ES211', 'Guipúzcoa': 'ES212', 'Gipuzkoa': 'ES212', 'Vizcaya': 'ES213', 'Bizkaia': 'ES213',
              'Comunidad Foral de Navarra': 'ES220', 'Navarra': 'ES220', 'La Rioja': 'ES230', 'Rioja, La': 'ES230',
              'Huesca': 'ES241', 'Teruel': 'ES242', 'Zaragoza': 'ES243', 'Comunidad de Madrid': 'ES300',
              'Madrid': 'ES300', 'Ávila': 'ES411', 'Burgos': 'ES412', 'León': 'ES413', 'Palencia': 'ES414',
              'Salamanca': 'ES415', 'Segovia': 'ES416', 'Soria': 'ES417', 'Valladolid': 'ES418', 'Zamora': 'ES419',
              'Albacete': 'ES421', 'Ciudad Real': 'ES422', 'Cuenca': 'ES423', 'Guadalajara': 'ES424', 'Toledo': 'ES425',
              'Badajoz': 'ES431', 'Cáceres': 'ES432', 'Barcelona': 'ES511', 'Gerona': 'ES512', 'Girona': 'ES512',
              'Lérida': 'ES513', 'Lleida': 'ES513', 'Tarragona': 'ES514', 'Alicante': 'ES521', 'Castellón': 'ES522',
              'Valencia': 'ES523', 'València': 'ES523', 'Islas Baleares': 'ES53', 'Ibiza y Formentera': 'ES531',
              'Mallorca': 'ES532', 'Menorca': 'ES533', 'Almería': 'ES611', 'Cádiz': 'ES612', 'Córdoba': 'ES613',
              'Granada': 'ES614', 'Huelva': 'ES615', 'Jaén': 'ES616', 'Málaga': 'ES617', 'Sevilla': 'ES618',
              'Región de Murcia': 'ES620', 'Murcia': 'ES620', 'Ceuta': 'ES630', 'Melilla': 'ES640',
              'El Hierro': 'ES703', 'Fuerteventura': 'ES704', 'Gran Canaria': 'ES705', 'La Gomera': 'ES706',
              'La Palma': 'ES707', 'Palmas, Las': 'ES707', 'Lanzarote': 'ES708', 'Tenerife': 'ES709',
              'Santa Cruz de Tenerife': 'ES709', None: None}


def parse_nuts(bidder_d):
    alias = bidder_d.get("provinciaDesCas")
    try:
        return ALIAS_NUTS[alias]
    except KeyError:
        logging.warning(f"No value for alias {alias}")
        return None


def parse_list_cpv(bidder_d):
    """ Returns the list of `tipoact` available for a given cbidder """
    return [act["codTipoActividad"] for act in bidder_d["listaActEconomicas"]]


def parse_list_serobr(bidder_d):
    """ Returns the list of `obras` and `servicios` available for a given cbidder """
    try:
        return sorted(set(flatten([['_'.join((i['grupo'], i['subgrupo'])) for i in bidder_d.get(k)] for k in
                                   ('listaObrasEstatal', 'listaObrasAutonomico', 'listaServiciosEstatal',
                                    'listaServiciosAutonomico')])))
    except TypeError:
        return None
