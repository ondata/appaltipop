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

schema_dir = "../schema"
schema_filename = "tender.schema.json"
schema = json.loads(f.read().replace('"#/', '"file:{}#/'.format(schema_filename)))
resolver = jsonschema.RefResolver(
    "file://{}/".format(
        Path(
            __file__,
            "../{}/".format(schema_dir)
        ).resolve()
    ),
    None
)

def docs(
    es_index_prefix,
    es_date_field,

    es_tender_buyer_field,
    es_tender_supplier_field,

    es_tender_id_field,
    es_buyer_id_field,
    es_supplier_id_field,

    es_buyer_fields,
    es_supplier_fields
):

    jsonl_path = Path(sys.argv[1])
    jsonl_files = jsonl_path.glob("*.jsonl") if jsonl_path.is_dir() else [jsonl_path]

    for jsonl_file in jsonl_files:
        if jsonl_file.is_file():
            with jsonlines.open(jsonl_file) as reader:
                for tender in reader:

                    try:
                        jsonschema.validate(tender, schema, resolver=resolver)
                    except Exception as ex:
                        logging.warning("Failed schema at {}, skip...".format(tender.get(es_tender_id_field)))
                        continue

                    logging.info("Indexing {} document...".format(tender[es_tender_id_field]))

                    yield {
                        "_op_type": "index",
                        "_index": "{}-tenders-{}".format(
                            es_index_prefix,
                            datetime.fromisoformat(tender[es_date_field].replace("Z", "+00:00")).year
                        ),
                        "_id": tender[es_tender_id_field],
                        "_source": tender
                    }

                    for buyer in tender.get(es_tender_buyer_field, []):

                        yield {
                            "_op_type": "create",
                            "_index": "{}-buyers".format(
                                es_index_prefix
                            ),
                            "_id": buyer[es_buyer_id_field],
                            "_source": { k: buyer[k] for k in es_buyer_fields.split(',') } if es_buyer_fields else buyer
                        }

                    for supplier in tender.get(es_tender_supplier_field, []):
                        yield {
                            "_op_type": "create",
                            "_index": "{}-suppliers".format(
                                es_index_prefix
                            ),
                            "_id": supplier[es_supplier_id_field],
                            "_source": { k: supplier[k] for k in es_supplier_fields.split(',') } if es_supplier_fields else supplier
                        }
        else:
            logging.warning("Failed to open {}, skip...".format(jsonl_file))


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

    es_tender_buyer_field = os.environ.get("ES_TENDER_BUYER_FIELD")
    es_tender_supplier_field = os.environ.get("ES_TENDER_SUPPLIER_FIELD")

    es_tender_id_field = os.environ.get("ES_TENDER_ID_FIELD")
    es_buyer_id_field = os.environ.get("ES_BUYER_ID_FIELD")
    es_buyer_fields = os.environ.get("ES_BUYER_FIELDS")

    es_supplier_id_field = os.environ.get("ES_SUPPLIER_ID_FIELD")
    es_supplier_fields = os.environ.get("ES_SUPPLIER_FIELDS")

    if not es_index_prefix or not es_date_field:
        logging.error("No ES_INDEX_PREFIX, ES_DATE_FIELD envars found")
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

            es_tender_buyer_field,
            es_tender_supplier_field,

            es_tender_id_field,
            es_buyer_id_field,
            es_supplier_id_field,

            es_buyer_fields,
            es_supplier_fields
        ),
        stats_only = True,
        raise_on_exception = False,
        raise_on_error = False
    )
