# Elasticsearch indexer

Take one or more [jsonl files](http://jsonlines.org/) and index all documents in Elasticsearch.

## Usage

Activate a pipenv shell and install all dependencies in the virtual environment: `pipenv shell` and `pipenv install`.

## Environment variables

All parameters are in the `.env` file provided and are available inside the virtual environment.

## Indices template

All documents are indexed in ES indices with a year-based name: `appaltipop-2014`, `appaltipop-2015`, and so on.

Indices with `appaltipop-` prefix will be indexed following the template in `template.json` file. To apply or update it: `curl -XPUT $ES_PROTOCOL://$ES_HOST/_template/appaltipop -H 'Content-Type: application/json' -d '@template.json'`.
