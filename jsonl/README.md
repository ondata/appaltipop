# Json to Jsonl converter

A simple converter to [jsonl format](http://jsonlines.org/) using [jsonlines module](https://jsonlines.readthedocs.io/en/latest/), much more performant as file source for Elasticsearch indexing.

## Validate

You can validate each line of (all) jsonl files against the `tender.schema.json` schema: `python validate.py [jsonl file]`.
