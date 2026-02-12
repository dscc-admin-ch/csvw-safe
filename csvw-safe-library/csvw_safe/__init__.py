from .make_metadata_from_data import make_metadata_from_data, main as make_metadata_from_data_main
from .make_dummy_from_metadata import make_dummy_from_metadata, main as make_dummy_from_metadata_main
from .validate_metadata import validate_metadata, main as validate_metadata_main
from .validate_metadata_shacl import validate_metadata_shacl, main as validate_metadata_shacl_main
from .assert_same_structure import assert_same_structure, main as assert_same_structure_main

__all__ = [
    "make_metadata_from_data",
    "make_metadata_from_data_main",
    "make_dummy_from_metadata",
    "make_dummy_from_metadata_main",
    "validate_metadata",
    "validate_metadata_main",
    "validate_metadata_shacl",
    "validate_metadata_shacl_main",
    "assert_same_structure",
    "assert_same_structure_main",
]