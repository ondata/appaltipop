import os
import sys
import json
import jsonlines
import jsonschema
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from pathlib import Path
import certifi
from datetime import datetime

logging.basicConfig(level=logging.INFO)

schema_dir = "../schema"
schema_filename = "tender.schema.json"

with open(Path(schema_dir, schema_filename)) as f:
    schema = json.loads(f.read().replace(
        '"#/', '"file:{}#/'.format(schema_filename)))

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
    es_tender_redflag_field,
    es_tender_redflagcount_field,
    es_tender_startdate_field,
    es_tender_enddate_field,
    es_tender_daterange_field,
    es_tender_duration_field,

    es_tender_id_field,
    es_buyer_id_field,
    es_buyer_name_field,
    es_buyer_resource_field,
    es_supplier_id_field,
    es_region_id_field,
    es_province_id_field,
    es_redflag_id_field,

    es_supplier_fields,
    es_region_fields,
    es_province_fields,

    buyers,
    resources
):

    jsonl_path = Path(sys.argv[1])
    jsonl_files = jsonl_path.glob(
        "*.jsonl") if jsonl_path.is_dir() else [jsonl_path]

    for jsonl_file in jsonl_files:
        if jsonl_file.is_file():
            with jsonlines.open(jsonl_file) as reader:
                for tender in reader:

                    try:
                        jsonschema.validate(tender, schema, resolver=resolver)
                    except Exception as ex:
                        logging.warning("Failed schema at {}, skip...".format(
                            tender.get(es_tender_id_field)
                        ))
                        continue

                    #logging.info("Indexing {} document...".format(tender[es_tender_id_field]))
                    tender[es_tender_redflagcount_field] = len(tender.get(es_tender_redflag_field, []))

                    if tender.get(es_tender_startdate_field) or tender.get(es_tender_enddate_field):
                        tender[es_tender_daterange_field] = {}
                        if tender.get(es_tender_startdate_field):
                            tender[es_tender_daterange_field]["gte"] = tender.get(es_tender_startdate_field)
                        if tender.get(es_tender_enddate_field):
                            if not tender.get(es_tender_startdate_field) or tender.get(es_tender_startdate_field) < tender.get(es_tender_enddate_field):
                                tender[es_tender_daterange_field]["lte"] = tender.get(es_tender_enddate_field)
                    
                    if tender.get(es_tender_startdate_field) and tender.get(es_tender_enddate_field):
                        tender[es_tender_duration_field] = (
                            datetime.fromisoformat(tender[es_tender_enddate_field].replace('Z', '')) - datetime.fromisoformat(tender[es_tender_startdate_field].replace('Z', ''))
                        ).days

                    for index, buyer in enumerate(tender.get(es_tender_buyer_field, [])):

                        if buyer.get(es_buyer_id_field) in buyers:

                            if tender[es_tender_buyer_field][index].get(es_buyer_name_field) and buyer.get(es_buyer_name_field):
                                if tender[es_tender_buyer_field][index].get(es_buyer_name_field) != buyer.get(es_buyer_name_field):
                                    tender[es_tender_buyer_field][index][es_buyer_name_field] = "{} ({})".format(
                                        tender[es_tender_buyer_field][index][es_buyer_name_field],
                                        buyer[es_buyer_name_field]
                                    )

                            tender[es_tender_buyer_field][index].update({
                                k: buyers[buyer[es_buyer_id_field]].get(k, "")
                                for k in (es_region_fields.split(',') + es_province_fields.split(','))
                            })

                            if buyer[es_buyer_id_field] in resources:
                                buyers[buyer[es_buyer_id_field]][es_buyer_resource_field] = resources.get(buyer[es_buyer_id_field])

                            yield {
                                "_op_type": "create",
                                "_index": "{}-buyers-{}".format(
                                    es_index_prefix,
                                    buyer[es_buyer_id_field][:2].lower()
                                ),
                                "_id": buyer[es_buyer_id_field],
                                "_source": buyers[buyer[es_buyer_id_field]]
                            }

                            if es_region_id_field and es_region_fields:

                                yield {
                                    "_op_type": "create",
                                    "_index": "{}-regions-{}".format(
                                        es_index_prefix,
                                        buyer[es_buyer_id_field][:2].lower()
                                    ),
                                    "_id": buyers[buyer[es_buyer_id_field]][es_region_id_field],
                                    "_source": {k: buyers[buyer[es_buyer_id_field]].get(k, "") for k in es_region_fields.split(',')}
                                }

                            if es_province_id_field and es_province_fields:

                                yield {
                                    "_op_type": "create",
                                    "_index": "{}-provinces-{}".format(
                                        es_index_prefix,
                                        buyer[es_buyer_id_field][:2].lower()
                                    ),
                                    "_id": buyers[buyer[es_buyer_id_field]][es_province_id_field],
                                    "_source": {k: buyers[buyer[es_buyer_id_field]].get(k, "") for k in es_province_fields.split(',')}
                                }

                        else:

                            if buyer.get(es_buyer_id_field) in resources:
                                buyer[es_buyer_resource_field] = resources[buyer[es_buyer_id_field]]

                            yield {
                                "_op_type": "create",
                                "_index": "{}-buyers-{}".format(
                                    es_index_prefix,
                                    buyer.get(es_buyer_id_field, "XX-XX")[:2].lower()
                                ),
                                **({ "_id": buyer[es_buyer_id_field] } if buyer.get(es_buyer_id_field) else {}),
                                "_source": buyer
                            }

                    for supplier in tender.get(es_tender_supplier_field, []):

                        yield {
                            "_op_type": "create",
                            "_index": "{}-suppliers-{}".format(
                                es_index_prefix,
                                supplier.get(es_supplier_id_field, "X-XX-XX")[2:4].lower()
                            ),
                            **({ "_id": supplier[es_supplier_id_field] } if supplier.get(es_supplier_id_field) else {}),
                            "_source": {k: supplier.get(k, "") for k in es_supplier_fields.split(',')} if es_supplier_fields else supplier
                        }

                    for redflag in tender.get(es_tender_redflag_field, []):

                        yield {
                            "_op_type": "create",
                            "_index": "{}-redflags".format(
                                es_index_prefix
                            ),
                            "_id": redflag[es_redflag_id_field],
                            "_source": redflag
                        }

                    yield {
                        "_op_type": "index",
                        "_index": "{}-tenders-{}".format(
                            es_index_prefix,
                            (
                                datetime.fromisoformat(
                                    tender[es_date_field].replace("Z", "+00:00")
                                ).year if tender.get(es_date_field) else "0000"
                            ) if tender.get(es_tender_id_field) else "xxxx"
                        ),
                        **({ "_id": tender[es_tender_id_field] } if tender.get(es_tender_id_field) else {}),
                        "_source": tender
                    }

        else:
            logging.warning("Failed to open {}, skip...".format(jsonl_file))


if __name__ == "__main__":

    if len(sys.argv) == 1:
        logging.error("Usage: {} /path/to/jsonl/folder/".format(sys.argv[0]))
        exit(1)

    es_scheme = os.environ.get("ES_SCHEME", "http")
    es_host = os.environ.get("ES_HOST", "localhost")
    es_port = os.environ.get(
        "ES_PORT") or 443 if es_scheme == "https" else 9200 if es_host == "localhost" else 80
    es_auth = (
        os.environ.get("ES_AUTH_USERNAME"),
        os.environ.get("ES_AUTH_PASSWORD")
    )

    es_index_prefix = os.environ.get("ES_INDEX_PREFIX")
    es_date_field = os.environ.get("ES_DATE_FIELD")

    es_tender_buyer_field = os.environ.get("ES_TENDER_BUYER_FIELD")
    es_tender_supplier_field = os.environ.get("ES_TENDER_SUPPLIER_FIELD")
    es_tender_redflag_field = os.environ.get("ES_TENDER_REDFLAG_FIELD")
    es_tender_redflagcount_field = os.environ.get("ES_TENDER_REDFLAGCOUNT_FIELD")
    es_tender_startdate_field = os.environ.get("ES_TENDER_STARTDATE_FIELD")
    es_tender_enddate_field = os.environ.get("ES_TENDER_ENDDATE_FIELD")
    es_tender_daterange_field = os.environ.get("ES_TENDER_DATERANGE_FIELD")
    es_tender_duration_field = os.environ.get("ES_TENDER_DURATION_FIELD")
    
    es_tender_id_field = os.environ.get("ES_TENDER_ID_FIELD")
    es_resource_id_field = os.environ.get("ES_RESOURCE_ID_FIELD")

    es_buyer_id_field = os.environ.get("ES_BUYER_ID_FIELD")
    es_buyer_name_field = os.environ.get("ES_BUYER_NAME_FIELD")
    es_buyer_resource_field = os.environ.get("ES_BUYER_RESOURCE_FIELD")

    es_supplier_id_field = os.environ.get("ES_SUPPLIER_ID_FIELD")
    es_supplier_fields = os.environ.get("ES_SUPPLIER_FIELDS")

    es_region_id_field = os.environ.get("ES_REGION_ID_FIELD")
    es_region_fields = os.environ.get("ES_REGION_FIELDS")

    es_province_id_field = os.environ.get("ES_PROVINCE_ID_FIELD")
    es_province_fields = os.environ.get("ES_PROVINCE_FIELDS")

    es_redflag_id_field = os.environ.get("ES_REDFLAG_ID_FIELD")

    if not es_index_prefix or not es_date_field:
        logging.error("No ES_INDEX_PREFIX, ES_DATE_FIELD envars found")
        exit(1)

    buyers_filename = "../json/buyers/buyers.json"
    with open(Path(buyers_filename)) as f:
        buyers = {buyer[es_buyer_id_field]: buyer for buyer in json.load(f)}

    resources_filename = "../data/download.json"
    with open(Path(resources_filename)) as f:
        resources = {resource[es_resource_id_field]: resource for resource in json.load(f)}

    es = Elasticsearch(
        [es_host],
        http_auth=es_auth,
        scheme=es_scheme,
        port=es_port,
        timeout=300
    )

    bulk(
        es,
        docs(
            es_index_prefix,
            es_date_field,

            es_tender_buyer_field,
            es_tender_supplier_field,
            es_tender_redflag_field,
            es_tender_redflagcount_field,
            es_tender_startdate_field,
            es_tender_enddate_field,
            es_tender_daterange_field,
            es_tender_duration_field,

            es_tender_id_field,
            es_buyer_id_field,
            es_buyer_name_field,
            es_buyer_resource_field,
            es_supplier_id_field,
            es_region_id_field,
            es_province_id_field,
            es_redflag_id_field,

            es_supplier_fields,
            es_region_fields,
            es_province_fields,

            buyers,
            resources
        ),
        stats_only=True,
        raise_on_exception=False,
        raise_on_error=False,

    )
