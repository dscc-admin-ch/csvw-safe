# API Reference

This page contains the automatically generated Python API documentation for the CSVW-SAFE library.

The API reference is generated directly from the source code docstrings using `mkdocstrings`.

---

# Core Workflows

## Metadata Generation

Automatically generate CSVW-SAFE metadata from tabular datasets.

::: csvw_safe.make_metadata_from_data

---

## Dummy Data Generation

Generate synthetic structural datasets from CSVW-SAFE metadata.

::: csvw_safe.make_dummy_from_metadata

---

## Metadata Validation

Validate metadata using the internal Pydantic schema.

::: csvw_safe.validate_metadata

---

## SHACL Validation

Validate metadata against RDF SHACL constraints.

::: csvw_safe.validate_metadata_shacl

---

## Structural Validation

Verify that generated dummy datasets preserve the expected structure.

::: csvw_safe.assert_same_structure

---

# Differential Privacy Integrations

## SmartNoise SQL Conversion

Convert CSVW-SAFE metadata into SmartNoise SQL configuration format.

::: csvw_safe.csvw_to_smartnoise_sql

---

## OpenDP Context Generation

Create OpenDP contexts directly from CSVW-SAFE metadata.

::: csvw_safe.csvw_to_opendp_context

---

## OpenDP Margin Utilities

Utilities for generating OpenDP margins from metadata.

::: csvw_safe.csvw_to_opendp_margins

---

# Metadata Models

## Metadata Structure

Core Pydantic metadata models used throughout the library.

::: csvw_safe.metadata_structure

---

## Datatypes

Datatype inference and datatype helper utilities.

::: csvw_safe.datatypes

---

## Constants

Shared constants used across the metadata pipeline.

::: csvw_safe.constants

---

# Data Generation Utilities

## Synthetic Series Generation

Utilities used internally to generate synthetic column values.

::: csvw_safe.generate_series

---

# Utility Functions

## General Utilities

Shared helper utilities used throughout the library.

::: csvw_safe.utils

---

# Suggested Reading Order

For most users, the recommended entry points are:

1. `csvw_safe.make_metadata_from_data`
2. `csvw_safe.validate_metadata`
3. `csvw_safe.make_dummy_from_metadata`
4. `csvw_safe.assert_same_structure`

For differential privacy integrations:

1. `csvw_safe.csvw_to_smartnoise_sql`
2. `csvw_safe.csvw_to_opendp_context`

---

# Notes

- API documentation is generated automatically from source docstrings.
- Type annotations and NumPy-style docstrings are rendered automatically.
- Internal helper functions may also appear when exported publicly.
- The documentation reflects the current state of the `main` branch.