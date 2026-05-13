"""
Top-level public interface for csvw_eo.

This module provides a simplified API by re-exporting the most commonly used
functions, classes, and constants for working with CSVW-style metadata.

It includes utilities to:

- Generate metadata from datasets
- Generate dummy datasets from metadata
- Validate metadata (standard and SHACL-based validation)
- Convert metadata to OpenDP and SmartNoise SQL contexts
- Assert structural equivalence between datasets
- Work with metadata models and datatypes
"""

from .assert_same_structure import assert_same_structure
from .constants import COL_LIST, COL_NAME, MAXIMUM, MINIMUM, TABLE_SCHEMA
from .csvw_to_opendp_context import csvw_to_opendp_context
from .csvw_to_smartnoise_sql import csvw_to_smartnoise_sql
from .datatypes import XSD_GROUP_MAP, DataTypesGroups, to_pandas_dtype
from .make_dummy_from_metadata import make_dummy_from_metadata
from .make_metadata_from_data import make_metadata_from_data
from .metadata_structure import ColumnMetadata, TableMetadata
from .validate_metadata import validate_metadata
from .validate_metadata_shacl import validate_metadata_shacl

__all__ = [  # noqa: RUF022
    # Core functionality
    "assert_same_structure",
    "csvw_to_opendp_context",
    "csvw_to_smartnoise_sql",
    "make_dummy_from_metadata",
    "make_metadata_from_data",
    "validate_metadata",
    "validate_metadata_shacl",
    # Metadata models
    "TableMetadata",
    "ColumnMetadata",
    # Constants
    "COL_LIST",
    "COL_NAME",
    "MAXIMUM",
    "MINIMUM",
    "TABLE_SCHEMA",
    # Datatypes
    "XSD_GROUP_MAP",
    "DataTypesGroups",
    "to_pandas_dtype",
]
