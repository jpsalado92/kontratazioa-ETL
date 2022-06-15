import logging

from scripts.transformers.t_utils import cast_bool


def parse_contracting_announcement_xml(soup):
    """ Part of 2021 and full 2022 go with this parser. Upcoming years expected to work with it """
    container = soup.find('contracting')
    cod_exp = p_cod_exp(container)
    url_id = soup.find('contractingAnnouncement')['id']
    d = {
        'cod_exp': cod_exp,
        'url_kontratazioa': f"https://www.contratacion.euskadi.eus/w32-kpeperfi/es/contenidos/"
                            f"anuncio_contratacion/{url_id}/es_doc/index.html"
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

    # p_budget(soup, d)
    # p_contracting_body(soup, d)
    # p_contracting_table(soup, d)
    # p_cpv(soup, d)
    # p_flags(soup, d)
    # p_simple_text(soup, d)
    return d


def p_type_tender(soup, d):
    try:
        d['type_tender'] = soup.find('contractingType').text
    except AttributeError:
        d['type_tender'] = None


def p_status_processing(soup, d):
    try:
        d['status_processing'] = soup.find('processingStatus').text
    except AttributeError:
        d['status_processing'] = None


def p_date_published(soup, d):
    try:
        d['status_processing'] = soup.find('firstPublicationDate').text
    except AttributeError:
        d['status_processing'] = None


def p_duration_contract(soup, d):
    try:
        d['duration_contract'] = ' '.join(
            (soup.find('contractPeriod').text,
             soup.find('contractPeriodType').text)
        )
    except AttributeError:
        pass


def p_type_processing(soup, d):
    try:
        d['type_processing'] = soup.find('processing').text
    except AttributeError:
        d['type_processing'] = None


def p_type_procedure(soup, d):
    try:
        d['type_procedure'] = soup.find('adjudicationProcedure').text
    except AttributeError:
        d['type_procedure'] = None


def p_cod_exp(soup):
    if soup.find('codExp'):
        return soup.find('codExp').text
    elif soup.find('idExpOrigen'):
        return soup.find('idExpOrigen').text


def p_description(soup, d):
    try:
        d['description'] = soup.find('subject').text
    except AttributeError:
        logging.warning("Unable to parse `description`")


def p_cauth_cod_perfil(soup, d):
    try:
        container = soup.find("contractingAuthority")
        d['cauth_cod_perfil'] = container["id"]
        d['cauth_name'] = container.find('name').text
    except AttributeError:
        logging.warning("Unable to parse `cauth`")


def p_is_european(soup, d):
    try:
        d['is_european'] = cast_bool(soup.find('placeExecutionInEU').text)
    except AttributeError:
        logging.warning("Unable to parse `is_european`")


def p_location_nuts(soup, d):
    try:
        for exec_place in soup.find('placeExecutionNUTS').children:
            if cast_bool(exec_place.find("main").text):
                d["location_nuts"] = exec_place["id"]
                break
    except AttributeError:
        logging.warning("Unable to parse `nuts`")


def p_flags(soup, d):
    """ This field must exist """
    flags_d = {}
    for flag in soup.find("flags").children:
        flags_d[flag['id']] = cast_bool(flag.text)
    d["flags"] = flags_d


def p_cpv(soup, d):
    cpv_list = []
    for cpv_container in soup.find('cpvs'):
        cpv_list.append(cpv_container["id"])
    d['cpvs'] = cpv_list


def p_promoter(soup, d):
    d["promoter_id"] = soup.find("entityDriving")["id"]
    d["promoter_name"] = soup.find("entityDriving").find("name").text


def p_organism(soup, d):
    try:
        d["organism_id"] = soup.find("contractingBody")["id"]
        d["organism_name"] = soup.find("contractingBody").find("name").text
    except (AttributeError, TypeError):
        d['organism_id'] = None
        d['organism_name'] = None


def p_budget(soup, d):
    text_entries = ["budgetWithoutVAT", "budgetWithVAT", "budgetEstimated", ]
    budget_d = {}
    for tag in text_entries:
        try:
            budget_d[tag] = soup.find(tag).text
        except AttributeError:
            pass
    d['budget'] = budget_d


def p_contracting_table(soup, d):
    ctable_d = {}
    try:
        ctable_d["contractingTable_id"] = soup.find("contractingTable")["id"]
    except (AttributeError, TypeError):
        pass
    try:
        comp_list = []
        for contact in soup.find("contractingTableComponents").children:
            comp_d = {"comp_id": contact["id"], "comp_name": contact.find("name").text,
                      "comp_function": contact.find("function").text, "comp_email": contact.find("email").text}
            comp_list.append(comp_d)
        ctable_d["contractingTable_components"] = comp_list
        d['contractingTable'] = ctable_d
    except (AttributeError, TypeError):
        pass
