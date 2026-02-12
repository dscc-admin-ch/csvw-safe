from .make_metadata_from_data import generate_csvw_dp_metadata, main as make_metadata_from_data_main
from .make_dummy_from_metadata import make_dummy_dataset_csvw_dp, main as make_dummy_from_metadata_main
from .validate_metadata import validate_metadata, run_shacl_validation, main as validate_metadata_main
from .assert_same_structure import assert_same_structure, main as assert_same_structure_main

__all__ = [
    "generate_csvw_dp_metadata",
    "make_metadata_from_data_main",
    "make_dummy_dataset_csvw_dp",
    "make_dummy_from_metadata_main",
    "validate_metadata",
    "run_shacl_validation",
    "validate_metadata_main",
    "assert_same_structure",
    "assert_same_structure_main",
]