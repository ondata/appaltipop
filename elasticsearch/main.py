import os
import sys
import jsonlines
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from pathlib import Path
import certifi
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def docs(es_index_prefix, es_date_field, es_id_field):

    jsonl_path = Path(sys.argv[1])

    with jsonlines.open(jsonl_path) as reader:
        for doc in reader:
            if doc.get(es_id_field):
                logging.info("Indexing {} document...".format(doc[es_id_field]))
                yield {
                    "_op_type": "index",
                    "_index": "{}-{}".format(
                        es_index_prefix,
                        datetime.fromisoformat(doc[es_date_field].replace("Z", "+00:00")).year
                    ),
                    "_id": doc[es_id_field],
                    "_source": doc
                }
            else:
                logging.warning("Missing \"{}\" property, skip...".format(es_id_field))


if __name__ == "__main__":

    if len(sys.argv) == 1:
        logging.error("Usage: {} /path/to/jsonl/file".format(sys.argv[0]))
        exit(1)

    es_scheme = os.environ.get("ES_SCHEME", "http")
    es_host = os.environ.get("ES_HOST", "localhost")
    es_port = os.environ.get("ES_PORT", 443 if es_scheme == "https" else 80)
    es_auth = (
        os.environ.get("ES_AUTH_USERNAME"),
        os.environ.get("ES_AUTH_PASSWORD")
    )

    es_index_prefix = os.environ.get("ES_INDEX_PREFIX")
    es_date_field = os.environ.get("ES_DATE_FIELD")
    es_id_field = os.environ.get("ES_ID_FIELD")

    if not es_index_prefix or not es_date_field:
        logging.error("No ES_INDEX_PREFIX, ES_DATE_FIELD or ES_ID_FIELD envars found")
        exit(1)

    es = Elasticsearch(
        [os.environ.get("ES_HOST","localhost")],
        http_auth = es_auth,
        scheme = es_scheme,
        port = es_port
    )

    bulk(
        es,
        docs(
            es_index_prefix,
            es_date_field,
            es_id_field
        )
    )
