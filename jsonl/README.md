# Json to Jsonl converter

A simple converter to [jsonl format](http://jsonlines.org/) using [jsonlines module](https://jsonlines.readthedocs.io/en/latest/), much more performant as file source for Elasticsearch indexing.

> Warning: jsonl files are not tracked by git

## Usage

Single file: `pipenv run python main.py /path/to/json/file [/path/to/jsonl/dir/]`.

Single folder: `pipenv run python main.py /path/to/json/dir/ [/path/to/jsonl/dir/]`.

Without `/path/to/jsonl/dir/` jsonl files will be saved in current folder.

## Validate

You can validate each line of (all) jsonl files against the `tender.schema.json` schema: `pipenv run python validate.py [/path/to/jsonl/file]`.
