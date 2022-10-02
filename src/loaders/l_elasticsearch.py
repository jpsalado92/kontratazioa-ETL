import configparser
import json
import os
from ssl import create_default_context

import elasticsearch.helpers
from elasticsearch import Elasticsearch


def document_stream(path, index_name):
    with open(path, "r", encoding='utf-8') as jsonl:
        for doc in jsonl:
            yield {"_index": index_name, "_source": json.loads(doc)}


def stream_bulk(es, fpath, index_name):
    stream = document_stream(fpath, index_name)
    for ok, response in elasticsearch.helpers.streaming_bulk(es, actions=stream):
        if not ok:
            print(response)


def connect_to_es(secrets_path):
    config = configparser.ConfigParser()
    config.read(os.path.join(secrets_path, 'secrets.cfg'))
    host = config['Elasticsearch']['Host']
    user = config['Elasticsearch']['User']
    password = config['Elasticsearch']['Password']
    cert = config['Elasticsearch']['Cert']
    es_session = Elasticsearch(
        host,
        ssl_context=create_default_context(cafile=os.path.join(secrets_path, cert)),
        basic_auth=(user, password)
    )
    if not es_session.ping():
        raise BaseException("Connection failed")
    return es_session


def load_in_es(jsonl_list, secrets_path):
    es = connect_to_es(secrets_path)
    for jsonl_path, idx_name in jsonl_list:
        stream_bulk(es, jsonl_path, idx_name)
