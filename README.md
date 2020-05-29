# AppaltiPOP

This repository is intended for project tracking. Here you can also find raw data and utilities for validation and indexing.

## Data

Pipeline:

- start: json files (an array of objects per source) in `json` folder
- then: jsonl files (same data, but one objects per line) in `jsonl` folder
- finally: indexing in `elasticsearch` folder

## Schema

You can validate all files using [JSON Schema](https://json-schema.org/) in `schema` folder. Refer to README files in each folder for further informations, you need Python 3 and virtual environments managed by [pipenv](https://pipenv.pypa.io/en/latest/).

General usage:

- `cd [folder]`
- `pipenv shell`
- `pipenv install` (only the first time)
- `python [script] [...args]` (inside the virtual env) or `pipenv run python [script] [...args]`
