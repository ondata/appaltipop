# Json data

Download current version of data in `tenders/` folder using [dvc](https://dvc.org/): `pipenv run dvc pull`.

> Warning: json files are not tracked by git

## Validation

You can validate each item of (all) json files against the `tender.schema.json` schema: `pipenv run python validate.py [json file]`.
