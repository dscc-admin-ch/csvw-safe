# TableMetadata (CSVW-EO)

## Overview

`TableMetadata` is the top-level container for describing a CSVW-EO table in the library. It aggregates:

- Global privacy constraints
- Column-level metadata
- Optional multi-column (group) metadata
- JSON-LD context information

It supports:
- **Serialization** to JSON-LD via `to_dict()`
- **Deserialization** from JSON-LD via `from_dict()`

---

## Fields

### Core Privacy Parameters

| Field | Type | Description |
|------|------|-------------|
| `privacy_unit` | `str \| None` | Identifier defining the unit of privacy (e.g., user ID). |
| `max_contributions` | `int \| None` | Maximum number of contributions per privacy unit across the dataset. |
| `max_length` | `int \| None` | Maximum number of rows per privacy unit. |
| `public_length` | `int \| None` | Total number of rows in the released dataset (public size). |


### Schema Definition

| Field | Type | Description |
|------|------|-------------|
| `columns` | `list[ColumnMetadata]` | List of column metadata definitions forming the table schema. |
| `column_groups` | `list[ColumnGroupMetadata] \| None` | Optional definitions for grouped columns sharing joint partitioning or constraints. |


### JSON-LD Metadata

| Field | Type | Description |
|------|------|-------------|
| `context` | `list[str]` | JSON-LD context. Defaults to `[CSVW_CONTEXT, CSVW_SAFE_CONTEXT]`. |
| `table_type` | `str` | JSON-LD type of the object. Defaults to `"Table"`. |


## `to_dict()` and `from_dict()` Representation

The `to_dict()` method converts the `TableMetadata` instance into a CSVW-EO compliant JSON-LD dictionary.

The `from_dict()` method reconstructs a `TableMetadata` instance from a CSVW-EO compliant JSON-LD dictionary.


### Field Mapping
| Python Attribute    | JSON Key                | Notes                       |
| ------------------- | ----------------------- | --------------------------- |
| `context`           | `@context`              | JSON-LD context array       |
| `table_type`        | `@type`                 | Typically `"Table"`         |
| `privacy_unit`      | `privacyUnit`           | May be `null`               |
| `max_contributions` | `maxContributions`      | May be `null`               |
| `max_length`        | `maxLength`             | May be `null`               |
| `public_length`     | `publicLength`          | May be `null`               |
| `columns`           | `tableSchema.columns`   | Always present              |
| `column_groups`     | `additionalInformation` | Only included if not `None` |

### Serialization (`to_dict()`)

- Produces a JSON-LD compliant dictionary
- Always includes:
  - `@context`
  - `@type`
  - `tableSchema.columns`
- Includes optional fields even if their value is `None`
- Serializes nested objects via:
  - `ColumnMetadata.to_dict()`
  - `ColumnGroupMetadata.to_dict()`


### Deserialization (`from_dict()`)

- Reconstructs a full `TableMetadata` object from a dictionary
- Parses nested structures:
  - Columns via `ColumnMetadata.from_dict()`
  - Column groups via `ColumnGroupMetadata.from_dict()`
- Uses `.get()` for optional fields, defaulting to `None` when missing
- Preserves JSON-LD metadata (`@context`, `@type`) if provided

#### Parsing Details

- `tableSchema.columns` is **required** and always parsed
- `additionalInformation` is **optional**
- Missing optional fields do **not** raise errors (handled by Pydantic defaults)
- (For now): Input is assumed to already follow CSVW-EO structure (no deep validation layer beyond model constraints). TODO: be more strict here (like datatypes options).


## Example

Python pydantic object:
```bash
metadata = TableMetadata(
    privacy_unit="user_id",
    max_contributions=5,
    max_length=10,
    public_length=1000,
    columns=[...]
)
```

Serialized json-ld output after `json_repr = metadata.to_dict()`:
```bash
json_repr = {
  "@context": [
    "http://www.w3.org/ns/csvw",
    "path/to/csvw-eo-context.jsonld"
  ],
  "@type": "Table",
  "privacyUnit": "user_id",
  "maxContributions": 5,
  "maxLength": 10,
  "publicLength": 1000,
  "tableSchema": {
    "columns": [
      {
        "@type": "Column",
        "name": "...",
        "datatype": "..."
      }
    ]
  }
}
```
and then the round-trip conversion:
```bash
metadata = TableMetadata.from_dict(json_repr)
```

## Library usage
- `make_metadata_from_data.py` generates a valid `TableMetadata` based on the input metadata and arguments and then serialise it with `metadata.to_dict()`.
- `validate_metadata.py` validates the json representation with the `from_dict()` method.
- Other scripts (`make_dummy_from_metadata.py`, `csvw_to_opendp_context.py`, `csvw_to_opendp_margins.py`, `csvw_to_smarntoise_sql.py`) expect the json representation as input but it is recommended to use `validate_metadata.py` on the json representation first to ensure that the scripts will work.


## Summary

`TableMetadata` is the entry point for CSVW-EO metadata, providing:
- A structured way to define dataset privacy constraints
- Full schema description (columns + groups)
- Bidirectional conversion:
  - to_dict() → JSON-LD serialization
  - from_dict() → object reconstruction

This makes it suitable for validation, transformation, and interoperability workflows.

