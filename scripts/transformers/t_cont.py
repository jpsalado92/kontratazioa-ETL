# Fields from the `.xml` document that may contain array-ed elements
XML_LIST_FIELDS = (
    'publicacionesBOTH', 'modificacionesIncidencia', 'publicacionesBOE',
    'publicacionesBOPV', 'clausulasEspeciales', 'anualidades', 'ivas', 'nutses',
    'prorrogas', 'modificacionesPlazo', 'modificacionesObjeto', 'finalizaciones',
    'lugaresAplicacion', 'resoluciones', 'publicacionesDOUE')

# Keys to be expected while parsing the `.xml` file
CONT_KNOWN_KEYS = [
    'cont_od_date_año', 'cont_cauth_cod_perfil', 'cont_od_id', 'cont_od_date_fecha_mod', 'objetoContratoEs',
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
            get_key(
                dict_obj=cont_d,
                possible_keys=["importeAdjudicacionSinIva", "presupuestoSinIva"]
            ),
        "cont_importe_con_iva":
            get_key(
                dict_obj=cont_d,
                possible_keys=["importeAdjudicacionConIva", "presupuestoConIva"]
            ),
        "cont_num_bidder": cont_d.get("numeroLicitadores"),

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
        "cont_cauth_cod_perfil": cont_d.get("cont_cauth_cod_perfil"),

        # Information related to the bidder linked to the CONT
        "cont_bidder_id":
            get_key(
                dict_obj=cont_d,
                possible_keys=["adjudicatario-identificacion", ]
            ),
        "cont_bidder_name":
            get_key(
                dict_obj=cont_d,
                possible_keys=["adjudicatario-razonSocial", ]
            ),
    }


def parse_xml_field(node, path='', dict_obj=None, list_fields=[], pref2remove=[]):
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

    if node_tag in list_fields:
        container = []
        for child in node:
            dd = {}
            parse_xml_field(child, '', dd, list_fields, pref2remove)
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
            parse_xml_field(child, new_path, dict_obj, list_fields, pref2remove)

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
