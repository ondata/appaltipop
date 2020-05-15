# AppaltiPOP data validator

A collection of data schemas following [JSON Schema specifications](https://json-schema.org/).

## Validation

You can validate AppaltiPOP documents using the [jsonschema python utility](https://python-jsonschema.readthedocs.io/en/stable/) provided.

Usage: `jsonschema -i [json file to test] [json schema file]`

- Buyer
  - Schema: `buyer.schema.json`
  - Correct example: `test/buyer.test.ok.json`
  - Wrong example: `test/buyer.test.ko.json`
  - Usage: `jsonschema -i [json to test] buyer.schema.json`

- Supplier
  - Schema: `supplier.schema.json`
  - Correct example: `test/supplier.test.ok.json`
  - Wrong example: `test/supplier.test.ko.json`
  - Usage: `jsonschema -i [json to test] supplier.schema.json`

- Tender
  - Schema: `tender.schema.json`
  - Correct example: `test/tender.test.ok.json`
  - Wrong example: `test/tender.test.ko.json`
  - Usage: `jsonschema -i [json to test] tender.schema.json`

You can also find schemas for collections of items:

- Buyers: `buyers.schema.json`
- Suppliers: `suppliers.schema.json`
- *Tenders: `tenders.schema.json` (defunct)*
