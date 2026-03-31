"""Validate metadata file format."""

import argparse
import json
from pathlib import Path
from typing import Any

from csvw_safe.metadata_structure import TableMetadata


def validate_metadata(metadata: dict[str, Any]) -> TableMetadata:
    """
    Validate CSVW-SAFE metadata against the pydantic model.

    Parameters
    ----------
    metadata : dict
        CSVW-SAFE metadata structure.
    """
    return TableMetadata.from_dict(metadata)


def main() -> None:
    """
    Command-line interface for SHACL validation of CSVW-SAFE metadata.

    This function parses command-line arguments specifying the metadata
    JSON-LD file and the SHACL shapes file, then runs SHACL validation.

    If validation succeeds, a success message is printed. If validation
    fails, the validation report is printed and the program exits with
    a non-zero status code.
    """
    parser = argparse.ArgumentParser(description="SHACL validation for CSVW-SAFE metadata")
    parser.add_argument("metadata_file", type=str)
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    validate_metadata(metadata)


if __name__ == "__main__":
    main()
