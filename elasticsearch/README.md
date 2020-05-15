# Elasticsearch indexer

Take a [jsonl file](http://jsonlines.org/) and index all documents in Elasticsearch.

## Usage

Activate a pipenv shell and install all dependencies in the virtual environment: `pipenv shell` and `pipenv install`.

## Environment variables

All parameters are in the `.env` file provided and are available inside the virtual environment.

## Indices template

All documents are indexed in ES indices with a year-based name: `appaltipop-2014`, `appaltipop-2015`, and so on.

Indices with `appaltipop-` prefix will be indexed following the template in `*.template.json` files. To apply or update them: `curl -XPUT $ES_SCHEME://$ES_HOST/_template/$ES_INDEX_PREFIX-* -H 'Content-Type: application/json' -d '@[name].template.json'` (replace `[name]` with `buyer`, `supplier` or `tender`).

## Delete all indices

Simple, but **dangerous** command: `curl -XDELETE "$ES_SCHEME://$ES_HOST/$ES_INDEX_PREFIX-*"`.
