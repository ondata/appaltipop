import os
import sys
import json
import jsonschema
import logging
from pathlib import Path

schema_dir = "../schema"
schema_filename = "tender.schema.json"
json_dir = "./tenders/"
resolver = jsonschema.RefResolver(
    "file://{}/".format(
        Path(
            __file__,
            "../{}/".format(schema_dir)
        ).resolve()
    ),
    None
)

with open("{}/{}".format(schema_dir, schema_filename)) as f_schema:
    schema = json.loads(f_schema.read().replace('"#/', '"file:{}#/'.format(schema_filename)))
    json_files = Path(json_dir).glob("*.json") if len(sys.argv) == 1 else [sys.argv[1]]
    for json_file in json_files:
        print("Validating {}...".format(json_file))
        with open(json_file) as f_data:
            tenders = json.load(f_data)
            for tender in tenders:
                try:
                    jsonschema.validate(tender, schema, resolver=resolver)
                except Exception as ex:
                    logging.error("Failed schema at {} in {}: {}".format(tender.get("ocds:releases/0/id"), json_file, ex))
                    exit(1)
