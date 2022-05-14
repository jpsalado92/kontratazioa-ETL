"""

"""
import calendar
import datetime
import json
import logging
import os
from itertools import product

import requests as r

DATA_URI = "https://www.contratacion.euskadi.eus/ac70cPublicidadWar/busquedaContrato/filter"

PAYLOAD = """
{{
    "draw": 1, 
    "filter": {{
        "contMenor": "{minor_contract}",
        "tipoEstadoContrato": {{
            "idEstadoContrato": "{id_contract_status}" 
        }},
        "tipoContrato": {{
            "codPerfil": "{id_contract_type}" 
        }},
        "fechaFormalizacionDesde": "{from_date}",
        "fechaFormalizacionHasta": "{to_date}",
        "_existeClausulaMedioAmb": "on",
        "_existeClausulaSocial": "on",
        "_existeClausulaIgualdad": "on"
    }},
    "rows": 250,
    "page": {page_num},
    "sidx": "ID_ESTADO_CONTRATO_119",
    "sord": "asc",
    "nd": 1643718969917
}}
"""

CONT_MENOR = {
    "true": "Es contrato menor",
    "false": "No es contrato menor",
}

TIPOS_CONTRATO = {
    "1": "Obras",
    "2": "Servicios",
    "3": "Suministros",
    "4": "Administrativos especiales",
    "5": "Concesión de obra pública",
    "6": "Concesión de servicios",
    "7": "Colaboración entre sector público y privado",
    "8": "Suscripción",
    "20": "Contrato mixto",
    "31": "Otros",
    "1041": "Privados"
}

ESTADOS_CONTRATO = {
    "4": "Ejecución",
    "5": "Finalizado"
}


def del_none(d):
    """
    Delete keys with the `null` value in a dictionary, recursively.
    """
    for key, value in list(d.items()):
        if not value:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d


def get_json_content(parameters):
    print(f"Fetching {parameters}")
    response = r.request("POST", DATA_URI, data=PAYLOAD.format(**parameters))
    content = response.content
    json_response = json.loads(content)
    return json_response


def get_full_pages_content(parameters: dict) -> list:
    parameters["page_num"] = 1
    json_response = get_json_content(parameters)
    if (not json_response) or ('rows' not in json_response):
        return []

    items = json_response["rows"]

    while int(json_response["total"]) > int(json_response["page"]):
        parameters["page_num"] += 1
        json_response = get_json_content(parameters)
        items.extend(json_response["rows"])

    return [del_none(item) for item in items]


def get_data(start_year, end_year, data_path, logs_filepath):
    for year, id_ec, id_tc, c_men in product(range(start_year, end_year),
                                             ESTADOS_CONTRATO,
                                             TIPOS_CONTRATO,
                                             CONT_MENOR):
        parameters = {
            "minor_contract": c_men,
            "id_contract_status": id_ec,
            "id_contract_type": id_tc,
        }

        # Get yearly contracts
        try:
            parameters["from_date"] = f"01/01/{year}"
            parameters["to_date"] = f"31/12/{year}"
            filename = get_filename(parameters)
            if not previous_record_exists(data_path, logs_filepath, filename):
                yearly_contracts = get_full_pages_content(parameters)
                store_json_contracts(yearly_contracts, parameters, filename, data_path)
            continue
        except json.JSONDecodeError:
            print("WARNING:Could not retrieve data in a YEARLY basis, changing to MONTHLY basis")

        # Get monthly contracts
        months = range(1, 13)
        for month in months:
            parameters["from_date"] = f"01/{format(month, '02')}/{year}"
            parameters["to_date"] = f"{format(calendar.monthrange(year, month)[1], '02')}/{format(month, '02')}/{year}"
            filename = get_filename(parameters)
            try:
                if not previous_record_exists(data_path, logs_filepath, filename):
                    monthly_contracts = get_full_pages_content(parameters)
                    store_json_contracts(monthly_contracts, parameters, filename, data_path)
                continue
            except json.JSONDecodeError:
                print("WARNING:Could not retrieve data in a MONTHLY basis, changing to DAILY basis")

            # Get daily contracts
            days = range(1, calendar.monthrange(year, month)[1])
            for day in days:
                parameters["from_date"] = f"{format(day, '02')}/{format(month, '02')}/{year}"
                parameters["to_date"] = parameters["from_date"]
                filename = get_filename(parameters)
                try:
                    if not previous_record_exists(data_path, logs_filepath, filename):
                        daily_contracts = get_full_pages_content(parameters)
                        store_json_contracts(daily_contracts, parameters, filename, data_path)
                    continue
                except json.JSONDecodeError:
                    print(f"WARNING: FailedRequest for {filename}")
                    logging.warning(f'FailedRequest:{filename}')


def reverse_str_items(string, separator):
    return '/'.join(reversed(string.split(separator)))


def get_filename(parameters):
    f_from_date = reverse_str_items(parameters["from_date"], '/').replace('/', '')
    f_to_date = reverse_str_items(parameters["to_date"], '/').replace('/', '')
    id_contract_status = parameters["id_contract_status"]
    id_contract_type = parameters["id_contract_type"]
    minor_contract = parameters["minor_contract"]
    return f'{f_from_date}-{f_to_date}_{id_contract_status}_{id_contract_type}_{minor_contract}.json'


def previous_record_exists(data_path, logs_filepath, filename):
    # Check if file already exists and continue if so
    if os.path.isfile(os.path.join(data_path, filename)):
        print(f"File found for {filename}")
        return True

    # Check if attempt is cataloged as empty in events.log
    with open(logs_filepath, "r") as log_file:
        if f'EmptyAttempt:{filename}' in log_file.read():
            print(f"EmptyAttempt found for {filename}")
            return True

    # Check if attempt is cataloged as unavailable in events.log
    with open(logs_filepath, "r") as log_file:
        if f'FailedRequest:{filename}' in log_file.read():
            print(f"FailedRequest found for {filename}")
            return True


def store_json_contracts(contracts, parameters, filename, data_path):
    # Check if data is available, continue if not
    if not contracts:
        logging.info(
            f'EmptyAttempt:{filename}')
        return

    # Serializing json
    data = {"contracts": contracts,
            "from_date": parameters["from_date"],
            "to_date": parameters["to_date"],
            "num_contracts": len(contracts),
            "id_contract_status": parameters["id_contract_status"],
            "id_contract_type": parameters["id_contract_type"],
            "minor_contract": parameters["minor_contract"]}

    json_object = json.dumps(data, indent=4)

    # Writing to `.json` file
    with open(os.path.join(data_path, filename), "w") as outfile:
        outfile.write(json_object)


def main():
    # Timestamp
    ts = datetime.datetime.now().strftime("%Y%m%d")

    # Directory management
    logs_path = os.path.join(os.getcwd(), 'logs')
    data_path = os.path.join(os.getcwd(), 'contracts', ts)
    for path in (logs_path, data_path):
        os.makedirs(path, exist_ok=True)

    # Enable logging
    logs_filepath = os.path.join(logs_path, f'{ts}-events.log')
    logging.basicConfig(filename=logs_filepath, filemode='a', encoding='utf-8',
                        level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Get data
    # get_data(2019, datetime.date.today().year + 1, data_path, logs_filepath)
    get_data(2000, 2018 + 1, data_path, logs_filepath)


if __name__ == "__main__":
    main()
