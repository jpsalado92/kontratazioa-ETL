import logging

from scripts.transformers.t_utils import cast_bool


def parse_record_xml(soup):
    """ Parses from 2019 to part of 2021 """
    container = soup.find('record')
    cod_exp = p_cod_exp(container)
    url_id = container['name'].lower().split('_')[1]
    d = {
        'cod_exp': cod_exp,
        'url_kontratazioa': "https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/"
                            f"anuncio_contratacion/exp{url_id}/es_doc/es_arch_exp{url_id}.html",
    }
    p_cauth_cod_perfil(container, d)
    p_description(container, d)
    p_promoter(container, d)
    p_organism(container, d)
    p_is_european(container, d)
    p_location_nuts(container, d)
    p_type_tender(container, d)
    p_status_processing(container, d)
    p_date_published(container, d)
    p_duration_contract(container, d)

    # p_date_awarding(container, d)
    # p_is_minor_contract(container, d)
    # p_list_awarded_bidders(container, d)
    # p_list_batches(container, d)
    # p_num_candidates(container, d)
    # p_type_procedure(container, d)
    # p_type_procedure_method(container, d)
    # p_type_processing(container, d)
    return d


def p_cod_exp(soup):
    try:
        return soup.find(attrs={"name": 'contratacion_expediente'}).text
    except AttributeError:
        return soup['name']


def p_cauth_cod_perfil(soup, d):
    try:
        container = soup.find(attrs={"name": 'contratacion_poder_adjudicador'})
        d['cauth_cod_perfil'] = container.find(attrs={"name": 'codigo'}).text
        d['cauth_name'] = container.find(attrs={"name": 'valor'}).text
    except AttributeError:
        logging.warning("Unable to parse cauth")


def p_promoter(soup, d):
    try:
        container = soup.find(attrs={"name": 'contratacion_entidad_impulsora'})
        d['promoter_id'] = container.find(attrs={"name": 'codigo'}).text
        d['promoter_name'] = container.find(attrs={"name": 'valor'}).text
    except AttributeError:
        d['promoter_id'] = None
        d['promoter_name'] = None


def p_organism(soup, d):
    try:
        container = soup.find(attrs={"name": 'contratacion_organo_contratacion'})
        d['organism_id'] = container.find(attrs={"name": 'codigo'}).text
        d['organism_name'] = container.find(attrs={"name": 'valor'}).text
    except AttributeError:
        d['organism_id'] = None
        d['organism_name'] = None


def p_type_tender(soup, d):
    try:
        d['type_tender'] = soup \
            .find(attrs={"name": 'contratacion_tipo_contrato'}) \
            .find(attrs={"name": 'valor'}) \
            .text
    except AttributeError:
        d['type_tender'] = None


def p_description(soup, d):
    try:
        d['description'] = soup.find(attrs={"name": 'contratacion_objeto_contrato'}).text
    except AttributeError:
        logging.warning("Unable to parse `description`")


def p_is_european(soup, d):
    try:
        d['is_european'] = cast_bool(soup.find(attrs={"name": 'lugar_ejecucion_principal_europa'}).text)
    except AttributeError:
        d['is_european'] = None


def p_location_nuts(soup, d):
    try:
        d['location_nuts'] = soup \
            .find(attrs={"name": 'lugar_ejecucion_principal'}) \
            .find(attrs={"name": 'codigo'}) \
            .text
    except AttributeError:
        d['location_nuts'] = None


def p_list_batches(soup, d):
    return NotImplemented


def p_num_candidates(soup, d):
    try:
        d['organism_name'] = int(soup.find(attrs={"name": 'contratacion_num_licitadores'}).text)
    except AttributeError:
        d['organism_name'] = None


def p_duration_contract(soup, d):
    try:
        d['duration_contract'] = soup.find(attrs={"name": 'contratacion_duracion_contrato_plazo_ejecucion'}).text
    except AttributeError:
        d['duration_contract'] = None


def p_type_processing(soup, d):
    try:
        d['type_processing'] = soup \
            .find(attrs={"name": 'contratacion_tramitacion'}) \
            .find(attrs={"name": 'valor'}) \
            .text
    except AttributeError:
        d['type_processing'] = None


def p_type_procedure(soup, d):
    try:
        d['type_procedure'] = soup \
            .find(attrs={"name": 'contratacion_procedimiento'}) \
            .find(attrs={"name": 'valor'}) \
            .text
    except AttributeError:
        d['type_procedure'] = None


def p_type_procedure_method(soup, d):
    try:
        d['type_procedure_method'] = soup \
            .find(attrs={"name": 'contratacion_forma_tramitacion'}) \
            .find(attrs={"name": 'valor'}) \
            .text
    except AttributeError:
        d['type_procedure_method'] = None


def p_is_minor_contract(soup, d):
    try:
        d['is_minor_contract'] = cast_bool(soup.find(attrs={"name": 'contratacion_contrato_menor'}).text)
    except AttributeError:
        d['is_minor_contract'] = None


def p_status_processing(soup, d):
    try:
        container = soup \
            .find(attrs={"name": 'contratacion'}) \
            .find(attrs={"name": 'contratacion_estado_tramitacion'})
        d['status_processing'] = container.find(attrs={"name": 'valor'}).text
    except AttributeError:
        d['status_processing'] = None


def p_date_published(soup, d):
    try:
        date = soup.find(attrs={"name": 'contratacion_fecha_adjudicacion_definitiva'}).text
        day, month, year = date.split(' ')[0]('/')
        d['date_awarding'] = '/'.join((year, month, day))
    except (TypeError, AttributeError):
        d['date_awarding'] = None


def p_date_awarding(soup, d):
    try:
        date = soup.find(attrs={"name": 'contratacion_fecha_adjudicacion_definitiva'}).text
        day, month, year = date.split('/')
        d['date_awarding'] = '/'.join((year, month, day))
    except AttributeError:
        d['date_awarding'] = None


def p_list_awarded_bidders(soup, d):
    return NotImplementedError
