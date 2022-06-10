import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import scripts.utils.utils as utils
from scripts.utils import log

# Fields from the `.xml` document that may contain array-ed elements
XML_LIST_FIELDS = (
    'publicacionesBOTH', 'modificacionesIncidencia', 'publicacionesBOE',
    'publicacionesBOPV', 'clausulasEspeciales', 'anualidades', 'ivas', 'nutses',
    'prorrogas', 'modificacionesPlazo', 'modificacionesObjeto', 'finalizaciones',
    'lugaresAplicacion', 'resoluciones', 'publicacionesDOUE'
)

# Keys to be expected while parsing the `.xml` file
CONT_KNOWN_KEYS = (
    'objetoContratoEs', 'objetoContratoEu',
    'codContrato', 'adjudicatario-identificacion', 'adjudicatario-razonSocial',
    'estadoContrato-descripcionEs', 'estadoContrato-descripcionEu', 'tipoContrato-descripcionEs',
    'tipoContrato-descripcionEu', 'estadoTramitacion-descripcionEs', 'estadoTramitacion-descripcionEu',
    'procedimientoAdjudicacion-descripcionEs', 'procedimientoAdjudicacion-descripcionEu',
    'criterioAdjudicacion-descripcionEs', 'criterioAdjudicacion-descripcionEu',
    'poderAdjudicador-descripciones-descripcionEs', 'poderAdjudicador-descripciones-descripcionEu',
    'poderAdjudicador-tipoAdministracion-descripcionEs', 'poderAdjudicador-tipoAdministracion-descripcionEu',
    'entidadImpulsora-descripcionEs', 'entidadImpulsora-descripcionEu', 'organoContratacion-descripcionEs',
    'organoContratacion-descripcionEu', 'entidadTramitadora-descripcionEs', 'entidadTramitadora-descripcionEu',
    'mesaContratacion-descripcionEs', 'mesaContratacion-descripcionEu', 'lugarEjecucionEuropea', 'fechaAdjudicacion',
    'fechaFirma', 'duracionContrato-duracion', 'duracionContrato-tipoDuracion-descripcionEs',
    'duracionContrato-tipoDuracion-descripcionEu', 'importeAdjudicacionSinIva', 'importeAdjudicacionConIva',
    'fechaFinContrato', 'plurianual', 'clausulasEspeciales', 'cpv-codCPV', 'cpv-descripciones-descripcionEs',
    'cpv-descripciones-descripcionEu', 'publicidad', 'publicacionesDOUE', 'fechaPublicacion', 'presupuestoSinIva',
    'presupuestoConIva', 'ivas', 'anualidades', 'numeroLicitadores', 'prorrogas',
    'modificacionesAdjudicatario-modificacionAdjudicatario-tipoCambio-descripcionEs',
    'modificacionesAdjudicatario-modificacionAdjudicatario-tipoCambio-descripcionEu',
    'modificacionesAdjudicatario-modificacionAdjudicatario-fechaAcuerdo',
    'modificacionesAdjudicatario-modificacionAdjudicatario-adjudicatario-razonSocial',
    'modificacionesAdjudicatario-modificacionAdjudicatario-fichero', 'codExp', 'ute', 'divisionLotes', 'nutses',
    'modificacionesPlazo', 'resoluciones', 'finalizaciones', 'publicacionesBOE', 'publicacionesBOPV',
    'modificacionesObjeto', 'modificacionesIncidencia', 'observacionesEs', 'observacionesEu',
    'duracionContrato-fechaDuracionFin', 'condicionesEjecucion-descripcionEs', 'condicionesEjecucion-descripcionEu',
    'continuidad', 'obligIndiProfAsig', 'reservadoTalleres', 'publicacionesBOTH', 'regimenGeneral-descripcionEs',
    'regimenGeneral-descripcionEu', 'revisionPrecios', 'garantias-garantias-garantiaContrato-fecha',
    'paises-lugar-codigo', 'paises-lugar-descripcionEs', 'paises-lugar-descripcionEu', 'paises-lugar-principal',
)


@log.start_end
def get_conts_file(path):
    """ Parses and cleans raw CONT `.xml` data and stores it in a CONT `.jsonl` file """
    jsonl_path = os.path.join(path, "conts.jsonl")
    with open(jsonl_path, 'w', encoding='utf-8') as jsonl:
        raw_cauth_conts_path = os.path.join(path, 'raw_cauth_conts')
        # Iterating through every CONT directory
        for xml_fname in os.listdir(raw_cauth_conts_path):
            cauth_cod_perfil, od_report_year, od_report_id, od_report_date_modified = xml_fname.removesuffix(
                '.xml').split('_')
            od_report_date_modified = datetime.strptime(od_report_date_modified, "%Y%m%d").strftime("%Y/%m/%d")
            cont_d = {'open_data_report': {'id': od_report_id,
                                           'year': od_report_year,
                                           'date_modified': od_report_date_modified},
                      'cauth_cod_perfil': str(int(cauth_cod_perfil))}
            # Iterating through every CONT in a given `.xml` file
            for xml_cont_node in ET.parse(os.path.join(raw_cauth_conts_path, xml_fname)).getroot():
                parsed_cont_d = parse_xml_cont_node(xml_cont_node)
                full_cont_d = dict(cont_d, **parsed_cont_d)
                jsonl.write(json.dumps(full_cont_d, ensure_ascii=False) + '\n')


def parse_xml_cont_node(xml_cont_node):
    parsed_cont_d = from_xml_to_dict(
        node=xml_cont_node,
        dict_obj={},
        array_fields=XML_LIST_FIELDS,
        pref2remove=['{com/ejie/ac70a/xml/opendata}', 'contratoOpenData']
    )
    utils.check_no_matched_key(parsed_cont_d, CONT_KNOWN_KEYS)
    cont_d = get_clean_cont(parsed_cont_d)
    return cont_d


def get_clean_cont(cont_d):
    """ Returns a dict object with selected CONT values """
    return {
        "cod_cont": cont_d.get("codContrato"),
        "cauth_promoter": cont_d.get("entidadImpulsora-descripcionEs"),
        "cauth_promoter_organism": cont_d.get("organoContratacion-descripcionEs"),
        "tender_cod_exp": cont_d.get("codContrato").split('_')[0],
        "bidder_cif": cont_d.get("adjudicatario-identificacion"),
        "bidder_name": cont_d.get("adjudicatario-razonSocial"),
        "location_nuts": [d["lugar-codigo"] for d in cont_d["nutses"]][0] if cont_d.get("nutses") else None,
        "location_foreign_country_code": cont_d.get("paises-lugar-codigo"),
        "date_signed": format_date(cont_d.get("fechaFirma")),
        "date_awarded": format_date(cont_d.get("fechaAdjudicacion")),
        "type_cont": get_cont_type(cont_d.get("tipoContrato-descripcionEs")),
        "type_procedure": cont_d.get("procedimientoAdjudicacion-descripcionEs"),
        "status_contract": cont_d.get("estadoContrato-descripcionEs"),
        "status_processing": cont_d.get("estadoTramitacion-descripcionEs"),
        "description": cont_d.get("objetoContratoEs"),
        "cpv": cont_d.get("cpv-codCPV"),
        "budget_with_vat": get_key(
            dict_obj=cont_d,
            possible_keys=["importeAdjudicacionConIva", "presupuestoConIva"]
        ),
        "is_european": cont_d.get("lugarEjecucionEuropea"),
        "is_ute": cont_d.get("ute"),
    }


ALIAS_TYPE_CONT = {
    'Administrativos especiales': 'Administrativos especiales',
    'Gestión de servicios públicos': 'Servicios',
    'Otros': 'OTROS',
    'null': 'INDETERMINADO',
    'Servicios': 'Servicios',
    'Gestión de servicios públicos/concesión de servicios': 'Servicios',
    'Concesión de servicios': 'Servicios',
    'Concesión de obra pública/concesión de obras': 'Obras',
    'Concesión de obra pública': 'Obras',
    'Colaboración entre sector público y sector privado': 'Colaboración publico-privada',
    'Suministros': 'Suministros',
    'Obras': 'Obras',
    'Privados': 'Privados',
    'Suscripción': 'Suscripción',
}


def get_cont_type(alias):
    if alias:
        try:
            return ALIAS_TYPE_CONT[alias]
        except KeyError:
            logging.warning(f"No value for alias {alias}")
            return 'OTROS'
    else:
        return None


def format_date(date):
    try:
        return date.replace('-', '/')
    except AttributeError:
        return None


def from_xml_to_dict(node, path='', dict_obj=None, array_fields=(), pref2remove=[]):
    """
    Given a `CONT` `.xml` nested object, recursively returns a plain dict object
    """
    if dict_obj is None:
        dict_obj = {}
    try:
        node_text = clean_xml_text(node.text)
    except:
        node_text = None

    node_tag = node.tag
    for pref in pref2remove:
        node_tag = node_tag.replace(pref, '')

    if path:
        new_path = '-'.join((path, node_tag))
    else:
        new_path = node_tag

    if node_tag in array_fields:
        container = []
        for child in node:
            dd = {}
            from_xml_to_dict(child, '', dd, array_fields, pref2remove)
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
            from_xml_to_dict(child, new_path, dict_obj, array_fields, pref2remove)

    return dict_obj


def get_key(dict_obj, possible_keys):
    for key in possible_keys:
        if dict_obj.get(key):
            return dict_obj[key]
    return None


def clean_xml_text(text: str):
    """ Format values according to their possible data type """

    # Clean line breaks and leading or ending spaces
    text = text.strip().replace('\n', '').strip()

    # If empty string return None
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
