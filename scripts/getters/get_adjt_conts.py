"""
Functions for fetching and storing `CONTs` (contracts) related to `ADJTs` (bidders)
from `www.contratación.euskadi.eus`.

The outputs from this module consist of the following files and directories:
    · `data\raw_pa_conts\<DATE>`:
        - Directory containing data scrapped in a given <DATE> from each `PA`
        (contracting authority) for every available YEAR in `.xml` format.

        - Sub-directories in this directory are named with the following logic:
            {pa_id}_{year}_{open_data_report_id}_{last_modified_date}

    · `<DATE>_adjt_conts.jsonl`:
        Consolidated data file containing data scrapped in a given <DATE>

Notes:
    · The following scrapper takes a huge amount of time to process files (around 50').
    · Async data fetching was implemented, but it was dismissed due to the problems the remote server
    presented while processing multiple requests. The code for it may be find in `archive\get_adjt_conts_async.py`


Useful resources:
    · Search for Open Data reports:
        - URL: https://www.contratacion.euskadi.eus/w32-kpetrans/es/ac70cPublicidadWar/busquedaInformesOpenData?locale=es
        - USE CASE:
            Through this resource one can download `CONTs` related to a given `ADJT` for
            a specific `YEAR` and `PA`. The output thrown by the website is named
            `zipInformeOpenData.zip` and contains:
                * A `.xlsx` data document in basque
                * A `.xlsx` data document in spanish
                * A `.xml` data document with content both in spanish and basque

"""
import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import requests

import log
import utils
from get_pas import get_pa_dict_list

SCOPE = "adjt_conts"

BASE_URL = "https://www.contratacion.euskadi.eus/"
ADJT_CONT_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar/indicadorREST" \
                           "/descargarInformeOpenData" \
                           "?idioma=es" \
                           "&anio={report_date}" \
                           "&idPoder={codperfil}" \
                           "&R01HNoPortal=true"

CONT_BY_PA_LIST_URL = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                                 "/busquedaInformesOpenData" \
                                 "/tablaInformes/filter"

# Fields from the `.xml` document that may contain array-ed elements
XML_LIST_FIELDS = (
    'publicacionesBOTH', 'modificacionesIncidencia', 'publicacionesBOE',
    'publicacionesBOPV', 'clausulasEspeciales', 'anualidades', 'ivas', 'nutses',
    'prorrogas', 'modificacionesPlazo', 'modificacionesObjeto', 'finalizaciones',
    'lugaresAplicacion', 'resoluciones', 'publicacionesDOUE')

# Keys to be expected while parsing the `.xml` file
CONT_KNOWN_KEYS = [
    'cont_od_date_año', 'cont_pa_cod_perfil', 'cont_od_id', 'cont_od_date_fecha_mod', 'objetoContratoEs',
    'objetoContratoEu', 'codContrato', 'adjudicatario-identificacion', 'adjudicatario-razonSocial',
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
    'regimenGeneral-descripcionEu', 'revisionPrecios', 'garantias-garantias-garantiaContrato-fecha'
]


@log.start_end
def get_cont_file(scope_path):
    """
    Parses and cleans raw CONT `.xml` data and stores it in a CONT `.jsonl` file
    """
    jsonl_path = os.path.join(scope_path, f"contratos.jsonl")
    with open(jsonl_path, 'w', encoding='utf-8') as jsonl:
        raw_pa_conts_path = os.path.join(scope_path, 'raw_pa_conts')
        # Iterating through every CONT directory
        for dirname in os.listdir(raw_pa_conts_path):
            dir_path = os.path.join(raw_pa_conts_path, dirname)
            pa_cod_perfil, cont_date, cont_id, cont_mod_date = dirname.split('_')
            pa_cod_perfil = str(int(pa_cod_perfil))

            # Iterating through every CONT in a given `.xml` file
            if len(os.listdir(dir_path)) != 2:
                logging.info(f"Missing files for {dirname}")
                continue

            for xml_cont in ET.parse(os.path.join(dir_path, dirname + '.xml')).getroot():
                cont_d = {"cont_od_date_año": cont_date, "cont_pa_cod_perfil": pa_cod_perfil,
                          "cont_od_id": cont_id, "cont_od_date_fecha_mod": cont_mod_date}
                parsed_cont_d = utils.parse_xml_field(
                    node=xml_cont,
                    dict_obj=cont_d,
                    list_fields=XML_LIST_FIELDS,
                    pref2remove=['{com/ejie/ac70a/xml/opendata}', 'contratoOpenData']
                )
                parsed_cont_d["codExp"], *_ = parsed_cont_d["codContrato"].split('_')
                utils.check_no_matched_key(parsed_cont_d, CONT_KNOWN_KEYS)
                clean_cont = get_clean_cont(parsed_cont_d)
                jsonl.write(json.dumps(clean_cont, ensure_ascii=False) + '\n')
    return jsonl_path


def get_clean_cont(cont_d):
    """ Returns a dict object with selected CONT values """
    return {
        # Information related to the Open Data Report
        "cont_odr_date_año": cont_d.get("cont_od_date_año"),
        "cont_odr_date_fecha_mod": cont_d.get("cont_od_date_fecha_mod"),
        "cont_odr_id": cont_d.get("cont_od_id"),

        # Information related to the CONT
        "cont_date_firma": cont_d.get("fechaFirma"),
        "cont_date_adjudicacion": cont_d.get("fechaAdjudicacion"),
        "cont_estado_contrato": {
            "es": cont_d.get("estadoContrato-descripcionEs"),
            "eu": cont_d.get("estadoContrato-descripcionEu"),
        },
        "cont_estado_tramitacion": {
            "es": cont_d.get("estadoTramitacion-descripcionEs"),
            "eu": cont_d.get("estadoTramitacion-descripcionEu"),
        },
        "cont_tipo_contrato": {
            "es": cont_d.get("tipoContrato-descripcionEs"),
            "eu": cont_d.get("tipoContrato-descripcionEu"),
        },
        "cont_procedimiento": {
            "es": cont_d.get("procedimientoAdjudicacion-descripcionEs"),
            "eu": cont_d.get("procedimientoAdjudicacion-descripcionEu"),
        },
        "cont_criterio": {
            "es": cont_d.get("criterioAdjudicacion-descripcionEs"),
            "eu": cont_d.get("criterioAdjudicacion-descripcionEu"),
        },
        "cont_bool_european": cont_d.get("lugarEjecucionEuropea"),
        "cont_bool_ute": cont_d.get("ute"),
        "cont_cod_exp": cont_d.get("codExp"),
        "cont_cod_contrato": cont_d.get("codContrato"),
        "cont_objeto": {
            "es": cont_d.get("objetoContratoEs"),
            "eu": cont_d.get("objetoContratoEu"),
        },
        "cont_cpv": cont_d.get("cpv-codCPV"),
        "cont_nuts": [d["lugar-codigo"] for d in cont_d["nutses"]] if cont_d.get("nutses") else None,
        "cont_importe_sin_iva":
            utils.get_key(
                dict_obj=cont_d,
                possible_keys=["importeAdjudicacionSinIva", "presupuestoSinIva"]
            ),
        "cont_importe_con_iva":
            utils.get_key(
                dict_obj=cont_d,
                possible_keys=["importeAdjudicacionConIva", "presupuestoConIva"]
            ),
        "cont_num_adjt": cont_d.get("numeroLicitadores"),

        # Information related to the PA linked to the CONT
        "cont_pa_descripcion": {
            "es": cont_d.get("poderAdjudicador-descripciones-descripcionEs"),
            "eu": cont_d.get("poderAdjudicador-descripciones-descripcionEu"),
        },
        "cont_pa_tipo": {
            "es": cont_d.get("poderAdjudicador-tipoAdministracion-descripcionEs"),
            "eu": cont_d.get("poderAdjudicador-tipoAdministracion-descripcionEu"),
        },
        "cont_pa_ei": {
            "es": cont_d.get("entidadImpulsora-descripcionEs"),
            "eu": cont_d.get("entidadImpulsora-descripcionEu"),
        },
        "cont_pa_ei_organo": {
            "es": cont_d.get("organoContratacion-descripcionEs"),
            "eu": cont_d.get("organoContratacion-descripcionEu"),
        },
        "cont_pa_cod_perfil": cont_d.get("cont_pa_cod_perfil"),

        # Information related to the ADJT linked to the CONT
        "cont_adjt_id":
            utils.get_key(
                dict_obj=cont_d,
                possible_keys=["adjudicatario-identificacion", ]
            ),
        "cont_adjt_name":
            utils.get_key(
                dict_obj=cont_d,
                possible_keys=["adjudicatario-razonSocial", ]
            ),
    }


@log.start_end
def get_raw_cont_files(scope_path):
    """ Fetch and store `.xml` and `.json` ADJT CONT data locally """
    pa_list = get_pa_dict_list()
    for pa_d in pa_list:
        pa_cod_perfil = pa_d['codPerfil']
        # Iterating through every ADJT CONT in a given list
        for cont in get_adjt_conts_by_pa(pa_cod_perfil=pa_cod_perfil):
            # Parsing basic information
            cont_date = str(int(cont['anioInforme']))
            cont_mod_date = cont['fechaModif'].replace('-', '')
            # ADJT CONT external id, as for the original website
            cont_ext_id = str(int(cont['idInformeOpendata']))
            # ADJT CONT internal id, as for this project
            cont_int_id = f"{int(pa_cod_perfil):05d}_{cont_date}_{cont_ext_id}_{cont_mod_date}"
            cont_fpath = os.path.join(scope_path, 'raw_pa_conts', cont_int_id)

            # Check if data already exists (both the `.json` file and the `.xml` file)
            if os.path.isdir(cont_fpath) and len(os.listdir(cont_fpath)) == 2:
                continue
            else:
                os.makedirs(cont_fpath, exist_ok=True)

            # Store `.json` response
            json_fpath = os.path.join(cont_fpath, cont_int_id + '.json')
            with open(json_fpath, mode='w', encoding='utf8') as jsonfile:
                jsonfile.write(json.dumps(cont, indent=2, ensure_ascii=False))

            # Fetch and extract `.zip` file to store `.xml` file
            zip_url = ADJT_CONT_URL.format(codperfil=pa_cod_perfil, report_date=cont_date)
            get_xml_file(url=zip_url, cont_path=cont_fpath, cont_id=cont_int_id)


@utils.retry(times=5, exceptions=json.decoder.JSONDecodeError, sleep=0.5)
def get_adjt_conts_by_pa(pa_cod_perfil):
    """ Fetch ADJT CONTs under a given PA as a list"""
    payload = \
        {
            "length": 1000000,
            "filter": {
                "poder": {"codPerfil": pa_cod_perfil},
                "anioDesde": "2000",
                "anioHasta": str(datetime.now().year)
            },
            "rows": 1000000,
            "page": 1
        }

    # Cookies to be used in the following request
    cookies_url = BASE_URL + "w32-kpetrans/es/ac70cPublicidadWar" \
                             "/busquedaInformesOpenData" \
                             "?locale=es"
    cookies = requests.get(cookies_url).headers['Set-Cookie']

    r_json = requests.post(CONT_BY_PA_LIST_URL,
                           headers={'Cookie': cookies},
                           data=json.dumps(payload),
                           timeout=25).json()

    if int(r_json['page']) > 1:
        print("More data available than expected!")
        raise

    return r_json["rows"]


@utils.retry(times=5, exceptions=BadZipFile, sleep=10)
def get_xml_file(url, cont_path, cont_id):
    xml_fpath = os.path.join(cont_path, cont_id + '.xml')
    r = requests.get(url)
    if not r:
        return

    with ZipFile(BytesIO(r.content)) as zipfile:
        zipped_filenames = zipfile.namelist()
        if len(zipped_filenames) > 3:
            raise BadZipFile
            logging.critical(f"Malformed zip file for: {url}")

        for file in zipped_filenames:
            if file.endswith('.xml'):
                zipfile.extract(file, cont_path)
                os.rename(os.path.join(cont_path, file.replace('"', '_')), xml_fpath)


def get_adjt_conts(operation_date, path):
    scope_path = os.path.join(path, operation_date, SCOPE)
    # Get adjt_conts data
    get_raw_cont_files(scope_path)
    # Consolidate adjt_conts data
    jsonl_path = get_cont_file(scope_path)
    return jsonl_path