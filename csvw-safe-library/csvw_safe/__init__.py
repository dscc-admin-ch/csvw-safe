"""
Top-level public interface for metadata utilities.

This module re-exports the primary functions used to:

- Generate metadata from datasets
- Generate dummy datasets from metadata
- Validate metadata (standard validation and SHACL-based validation)
- Assert structural equivalence between datasets

Both library functions and CLI entry-point wrappers (`*_main`) are included
in the public API.
"""

from .assert_same_structure import assert_same_structure
from .assert_same_structure import main as assert_same_structure_main
from .csvw_to_opendp_context import csvw_to_opendp_context
from .csvw_to_smartnoise_sql import csvw_to_smartnoise_sql
from .csvw_to_smartnoise_sql import main as csvw_to_smartnoise_sql_main
from .make_dummy_from_metadata import main as make_dummy_from_metadata_main
from .make_dummy_from_metadata import make_dummy_from_metadata
from .make_metadata_from_data import main as make_metadata_from_data_main
from .make_metadata_from_data import make_metadata_from_data
from .metadata_structure import TableMetadata
from .validate_metadata import main as validate_metadata_main
from .validate_metadata import validate_metadata
from .validate_metadata_shacl import main as validate_metadata_shacl_main
from .validate_metadata_shacl import validate_metadata_shacl

__all__ = [
    "assert_same_structure",
    "assert_same_structure_main",
    "csvw_to_opendp_context",
    "csvw_to_smartnoise_sql",
    "csvw_to_smartnoise_sql_main",
    "make_dummy_from_metadata",
    "make_dummy_from_metadata_main",
    "make_metadata_from_data",
    "make_metadata_from_data_main",
    "validate_metadata",
    "validate_metadata_main",
    "validate_metadata_shacl",
    "validate_metadata_shacl_main",
    "TableMetadata",
]
