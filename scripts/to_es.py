import configparser
import json
import os
from ssl import create_default_context

import elasticsearch.helpers
from elasticsearch import Elasticsearch

SECRETS_PATH = os.path.join('..', 'secrets')
config = configparser.ConfigParser()
config.read(os.path.join(SECRETS_PATH, "secrets.cfg"))

ELASTIC_HOST = config['Elasticsearch']['Host']
ELASTIC_USER = config['Elasticsearch']['User']
ELASTIC_PASSWORD = config['Elasticsearch']['Password']
ELASTIC_CERT = config['Elasticsearch']['Cert']


def connect_to_es():
    es_session = Elasticsearch(
        ELASTIC_HOST,
        ssl_context=create_default_context(cafile=os.path.join(SECRETS_PATH, ELASTIC_CERT)),
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
    )
    if not es_session.ping():
        raise BaseException("Connection failed")
    return es_session


def document_stream(path, index_name):
    with open(path, "r", encoding='utf-8') as jsonl:
        for doc in jsonl:
            yield {"_index": index_name, "_source": json.loads(doc)}


def stream_bulk(es, file, index_name):
    stream = document_stream(file, index_name)
    for ok, response in elasticsearch.helpers.streaming_bulk(es, actions=stream):
        if not ok:
            print(response)


if __name__ == "__main__":
    es = connect_to_es()
    file = os.path.join('..', 'data', 'contratos', '20220307_contratos.jsonl')
    index_name = "contratos2"
    stream_bulk(es, file, index_name)
