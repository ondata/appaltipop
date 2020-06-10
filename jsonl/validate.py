import os
import sys
import json
import jsonschema
import jsonlines
import logging
from pathlib import Path

schema_dir = "../schema"
schema_filename = "tender.schema.json"
jsonl_dir = "./"
resolver = jsonschema.RefResolver(
    "file://{}/".format(
        Path(
            __file__,
            "../{}/".format(schema_dir)
        ).resolve()
    ),
    None
)

with open("{}/{}".format(schema_dir, schema_filename)) as f:
    schema = json.loads(f.read().replace('"#/', '"file:{}#/'.format(schema_filename)))
    jsonl_files = Path(jsonl_dir).glob("*.jsonl") if len(sys.argv) == 1 else [sys.argv[1]]
    for jsonl_file in jsonl_files:
        print("Validating {}...".format(jsonl_file))
        with jsonlines.open(jsonl_file) as reader:
            for tender in reader:
                try:
                    jsonschema.validate(tender, schema, resolver=resolver)
                except Exception as ex:
                    logging.error(
                        "Failed schema at {} in {}: {}".format(
                            tender.get("cig"), jsonl_file, ex
                        )
                    )
                    exit(1)
