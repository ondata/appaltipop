# Elasticsearch indexer

Take a [jsonl file](http://jsonlines.org/) or a folder containing jsonl files and index all documents in Elasticsearch.

## Usage

Activate a pipenv shell and install all dependencies in the virtual environment: `pipenv shell` and `pipenv install`.

Run indexing:

- Single file: `pipenv run main.py /path/to/jsonl/file`
- Folder: `pipenv run main.py /path/to/jsonl/folder/`

## Environment variables

All parameters are in the `.env` file provided and are available inside the virtual environment.

## Indices template

All documents are indexed in ES indices with a year-based name: `appaltipop-tenders-2014`, `appaltipop-tenders-2015`, and so on. If `ocds:releases/0/tender/contractPeriod/startDate` attribute is missing, index will be `appaltipop-tenders-0000`.

Indices with `appaltipop-` prefix will be indexed following the template in `*.template.json` files. To apply or update them: `curl -XPUT $ES_SCHEME://$ES_HOST:$ES_PORT/_template/$ES_INDEX_PREFIX-[name] -H 'Content-Type: application/json' -d '@[name].template.json'` (replace `[name]` with `buyer`, `supplier` or `tender`).

## Delete all indices

Simple, but **dangerous** command: `curl -XDELETE "$ES_SCHEME://$ES_HOST/$ES_INDEX_PREFIX-*"`.
