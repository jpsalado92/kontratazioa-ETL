import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import requests

from get_pa import get_pa_dict_list
from utils import retry

SCOPE = "contratos"
DATA_PATH = os.path.join(os.getcwd(), '..', 'data', SCOPE)
TIME_STAMP = datetime.now().strftime("%Y%m%d")

BASE_URL = "https://www.contratacion.euskadi.eus/"
COOKIES_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                         "/busquedaInformesOpenData" \
                         "?locale=es"
COOKIES = requests.get(COOKIES_URL).headers['Set-Cookie']

CONT_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar/indicadorREST" \
                      "/descargarInformeOpenData" \
                      "?idioma=es" \
                      "&anio={report_date}" \
                      "&idPoder={codperfil}" \
                      "&R01HNoPortal=true"
CONT_BY_PA_LIST_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                                 "/busquedaInformesOpenData" \
                                 "/tablaInformes/filter"

XML_LIST_FIELDS = (
    'publicacionesBOTH', 'modificacionesIncidencia', 'publicacionesBOE',
    'publicacionesBOPV', 'clausulasEspeciales', 'anualidades', 'ivas', 'nutses',
    'prorrogas', 'modificacionesPlazo', 'modificacionesObjeto', 'finalizaciones',
    'lugaresAplicacion', 'resoluciones', 'publicacionesDOUE')

CONT_KNOWN_KEYS = ['cont_od_date_año', 'cont_pa_cod_perfil', 'cont_od_id', 'cont_od_date_fecha_mod', 'objetoContratoEs',
                   'objetoContratoEu', 'codContrato', 'adjudicatario-identificacion', 'adjudicatario-razonSocial',
                   'estadoContrato-descripcionEs', 'estadoContrato-descripcionEu', 'tipoContrato-descripcionEs',
                   'tipoContrato-descripcionEu', 'estadoTramitacion-descripcionEs', 'estadoTramitacion-descripcionEu',
                   'procedimientoAdjudicacion-descripcionEs', 'procedimientoAdjudicacion-descripcionEu',
                   'criterioAdjudicacion-descripcionEs', 'criterioAdjudicacion-descripcionEu',
                   'poderAdjudicador-descripciones-descripcionEs', 'poderAdjudicador-descripciones-descripcionEu',
                   'poderAdjudicador-tipoAdministracion-descripcionEs',
                   'poderAdjudicador-tipoAdministracion-descripcionEu', 'entidadImpulsora-descripcionEs',
                   'entidadImpulsora-descripcionEu', 'organoContratacion-descripcionEs',
                   'organoContratacion-descripcionEu', 'entidadTramitadora-descripcionEs',
                   'entidadTramitadora-descripcionEu', 'mesaContratacion-descripcionEs',
                   'mesaContratacion-descripcionEu', 'lugarEjecucionEuropea', 'fechaAdjudicacion', 'fechaFirma',
                   'duracionContrato-duracion', 'duracionContrato-tipoDuracion-descripcionEs',
                   'duracionContrato-tipoDuracion-descripcionEu', 'importeAdjudicacionSinIva',
                   'importeAdjudicacionConIva', 'fechaFinContrato', 'plurianual', 'clausulasEspeciales', 'cpv-codCPV',
                   'cpv-descripciones-descripcionEs', 'cpv-descripciones-descripcionEu', 'publicidad',
                   'publicacionesDOUE', 'fechaPublicacion', 'presupuestoSinIva', 'presupuestoConIva', 'ivas',
                   'anualidades', 'numeroLicitadores', 'prorrogas',
                   'modificacionesAdjudicatario-modificacionAdjudicatario-tipoCambio-descripcionEs',
                   'modificacionesAdjudicatario-modificacionAdjudicatario-tipoCambio-descripcionEu',
                   'modificacionesAdjudicatario-modificacionAdjudicatario-fechaAcuerdo',
                   'modificacionesAdjudicatario-modificacionAdjudicatario-adjudicatario-razonSocial',
                   'modificacionesAdjudicatario-modificacionAdjudicatario-fichero', 'codExp', 'ute', 'divisionLotes',
                   'nutses', 'modificacionesPlazo', 'resoluciones', 'finalizaciones', 'publicacionesBOE',
                   'publicacionesBOPV', 'modificacionesObjeto', 'modificacionesIncidencia', 'observacionesEs',
                   'observacionesEu', 'duracionContrato-fechaDuracionFin', 'condicionesEjecucion-descripcionEs',
                   'condicionesEjecucion-descripcionEu', 'continuidad', 'obligIndiProfAsig', 'reservadoTalleres',
                   'publicacionesBOTH', 'regimenGeneral-descripcionEs', 'regimenGeneral-descripcionEu',
                   'revisionPrecios', 'garantias-garantias-garantiaContrato-fecha']


def get_raw_cont_files(date=TIME_STAMP):
    """ Fetch and store `xml` and `json` CONT data locally """
    for pa_d in get_pa_dict_list():
        pa_cod_perfil = pa_d['codPerfil']

        for cont in get_cont_list_by_pa(pa_cod_perfil=pa_cod_perfil):
            cont_date = str(int(cont['anioInforme']))
            cont_mod_date = cont['fechaModif'].replace('-', '')
            cont_id = str(int(cont['idInformeOpendata']))
            cont_fname = f"{int(pa_cod_perfil):05d}_{cont_date}_{cont_id}_{cont_mod_date}"
            cont_fpath = os.path.join(DATA_PATH, 'raw_pa_conts', date, cont_fname)
            if os.path.isdir(cont_fpath):
                continue

            os.makedirs(cont_fpath, exist_ok=True)
            json_fpath = os.path.join(cont_fpath, cont_fname + '.json')
            with open(json_fpath, mode='w', encoding='utf8') as file:
                del cont['poder']
                file.write(json.dumps(cont, indent=2, ensure_ascii=False))

            # Fetch, store and extract zip file
            zip_url = CONT_URL.format(codperfil=pa_cod_perfil, report_date=cont_date)
            try:
                r = requests.get(zip_url)
                xml_fpath = os.path.join(cont_fpath, cont_fname + '.xml')
                with ZipFile(BytesIO(r.content)) as zfile:
                    for file in zfile.namelist():
                        if file.endswith('.xml'):
                            zfile.extract(file, cont_fpath)
                            os.rename(os.path.join(cont_fpath, file.replace('"', '_')), xml_fpath)
            except BadZipFile:
                print(zip_url)


@retry(times=5, exceptions=json.decoder.JSONDecodeError, sleep=0.5)
def get_cont_list_by_pa(pa_cod_perfil):
    """ Fetch CONTs under a given PA as a list"""
    payload = \
        {
            "length": 1000000,
            "filter": {
                "poder": {"codPerfil": pa_cod_perfil},
                "anioDesde": "2000",
                "anioHasta": "2030"
            },
            "rows": 1000000,
            "page": 1
        }
    r_json = requests.post(CONT_BY_PA_LIST_URL,
                           headers={'Cookie': COOKIES},
                           data=json.dumps(payload),
                           timeout=25).json()

    if int(r_json['page']) > 1:
        print("More data available than expected!")
        raise

    return r_json["rows"]


def get_cont_file(date=TIME_STAMP):
    """ Parses and cleans raw CONT xml data and stores it in a CONT jsonl file """
    with open(os.path.join(DATA_PATH, f"{date}_contratos.jsonl"), 'w') as contfile:
        raw_pa_conts_path = os.path.join(DATA_PATH, 'raw_pa_conts', date)
        # Iterating through every CONT directory
        for dirname in os.listdir(raw_pa_conts_path):
            dir_path = os.path.join(raw_pa_conts_path, dirname)
            pa_cod_perfil, cont_date, cont_id, cont_mod_date = dirname.split('_')
            pa_cod_perfil = str(int(pa_cod_perfil))

            # Iterating through every CONT in a given `xml` file
            if len(os.listdir(dir_path)) != 2:
                continue
            for xml_cont in ET.parse(os.path.join(dir_path, dirname + '.xml')).getroot():
                cont_d = {"cont_od_date_año": cont_date, "cont_pa_cod_perfil": pa_cod_perfil,
                          "cont_od_id": cont_id, "cont_od_date_fecha_mod": cont_mod_date}
                parsed_cont_d = parse_xml_field(node=xml_cont, dict_obj=cont_d)
                parsed_cont_d["codExp"], *_ = parsed_cont_d["codContrato"].split('_')
                check_no_matched_key(parsed_cont_d)
                clean_cont = get_clean_cont(parsed_cont_d)
                contfile.write(json.dumps(clean_cont, ensure_ascii=False) + '\n')


def get_clean_cont(cont_d):
    """ Returns a dict object with selected CONT values """
    return {
        "cont_od_date_año": cont_d.get("cont_od_date_año"),
        # "cont_od_date_fecha_mod": cont_d.get("cont_od_date_fecha_mod"),
        # "cont_od_id": cont_d.get("cont_od_id"),
        "cont_date_firma": cont_d.get("fechaFirma"),
        "cont_date_adjudicacion": cont_d.get("fechaAdjudicacion"),
        "cont_estado_contrato_es": cont_d.get("estadoContrato-descripcionEs"),
        # "cont_estado_contrato_eu": cont_d.get("estadoContrato-descripcionEu"),
        "cont_estado_tramitacion_es": cont_d.get("estadoTramitacion-descripcionEs"),
        # "cont_estado_tramitacion_eu": cont_d.get("estadoTramitacion-descripcionEu"),
        "cont_tipo_contrato_es": cont_d.get("tipoContrato-descripcionEs"),
        # "cont_tipo_contrato_eu": cont_d.get("tipoContrato-descripcionEu"),
        "cont_procedimiento_es": cont_d.get("procedimientoAdjudicacion-descripcionEs"),
        # "cont_procedimiento_eu": cont_d.get("procedimientoAdjudicacion-descripcionEu"),
        # "cont_criterio_es": cont_d.get("criterioAdjudicacion-descripcionEs"),
        # "cont_criterio_eu": cont_d.get("criterioAdjudicacion-descripcionEu"),
        # "cont_bool_lugar_ejecucion_europa": cont_d.get("lugarEjecucionEuropea"),
        # "cont_bool_ute": cont_d.get("ute"),
        "cont_pa_cod_perfil": cont_d.get("cont_pa_cod_perfil"),
        # "cont_pa_descripcion_es": cont_d.get("poderAdjudicador-descripciones-descripcionEs"),
        # "cont_pa_descripcion_eu": cont_d.get("poderAdjudicador-descripciones-descripcionEu"),
        "cont_pa_tipo_es": cont_d.get("poderAdjudicador-tipoAdministracion-descripcionEs"),
        # "cont_pa_tipo_eu": cont_d.get("poderAdjudicador-tipoAdministracion-descripcionEu"),
        "cont_pa_ei_es": cont_d.get("entidadImpulsora-descripcionEs"),
        # "cont_pa_ei_eu": cont_d.get("entidadImpulsora-descripcionEu"),
        "cont_pa_ei_organo_es": cont_d.get("organoContratacion-descripcionEs"),
        # "cont_pa_ei_organo_eu": cont_d.get("organoContratacion-descripcionEs"),
        "cont_adjt_id": get_adjt_id(cont_d),
        "cont_adjt_name": get_adjt_name(cont_d),
        # "cont_cod_exp": cont_d.get("codExp"),
        # "cont_cod_contrato": cont_d.get("codContrato"),
        # "cont_objeto_es": cont_d.get("objetoContratoEs"),
        # "cont_objeto_eu": cont_d.get("objetoContratoEu"),
        "cont_cpv": cont_d.get("cpv-codCPV"),
        "cont_nuts": get_nuts(cont_d),
        "cont_importe_sin_iva": get_cont_importe_sin_iva(cont_d),
        "cont_importe_con_iva": get_cont_importe_con_iva(cont_d),
        # "cont_num_adjt": cont_d.get("numeroLicitadores"),
    }


def get_adjt_name(cont_d):
    """ Gets `adjt_name` value for a given CONT """
    if cont_d.get("adjudicatario-razonSocial"):
        return cont_d["adjudicatario-razonSocial"].upper().strip()
    else:
        return None


def get_adjt_id(cont_d):
    """ Gets `adjt_id` value for a given CONT """
    if cont_d.get("adjudicatario-identificacion"):
        return str(cont_d["adjudicatario-identificacion"]).upper().strip()
    else:
        return None


def get_nuts(cont_d):
    """ Gets `nuts` value for a given CONT """
    if cont_d.get("nutses"):
        return [d["lugar-codigo"] for d in cont_d["nutses"]]
    else:
        return None


def get_cont_importe_sin_iva(cont_d):
    """ Gets `cont_importe_sin_iva` value for a given CONT """
    if cont_d.get("importeAdjudicacionSinIva"):
        return cont_d["importeAdjudicacionSinIva"]
    elif cont_d.get("presupuestoSinIva"):
        return cont_d["presupuestoSinIva"]
    else:
        return None


def get_cont_importe_con_iva(cont_d):
    """ Gets `cont_importe_con_iva` value for a given CONT """
    if cont_d.get("importeAdjudicacionConIva"):
        return cont_d["importeAdjudicacionConIva"]
    elif cont_d.get("presupuestoConIva"):
        return cont_d["presupuestoConIva"]
    else:
        return None


def parse_xml_field(node, path='', dict_obj=None):
    """ Given a xml nested object recursively returns a plain dict object """
    if dict_obj is None:
        dict_obj = {}
    try:
        node_text = clean_xml_text(node.text)
    except:
        node_text = None

    node_tag = node.tag.replace('{com/ejie/ac70a/xml/opendata}', '').replace('contratoOpenData', '')
    if path:
        new_path = '-'.join((path, node_tag))
    else:
        new_path = node_tag

    if node_tag in XML_LIST_FIELDS:
        container = []
        for child in node:
            dd = {}
            parse_xml_field(child, '', dd)
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
            parse_xml_field(child, new_path, dict_obj)

    return dict_obj


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


def check_no_matched_key(cont_d):
    """ Raises an exception if unknown keys are found in the dict object """
    if not all([k in CONT_KNOWN_KEYS for k in cont_d]):
        unknown_keys = set(cont_d.keys()) - set(CONT_KNOWN_KEYS)
        raise KeyError(f"The following concepts are not under known cont-keys: {unknown_keys}")


if __name__ == "__main__":
    get_raw_cont_files()
    get_cont_file()
