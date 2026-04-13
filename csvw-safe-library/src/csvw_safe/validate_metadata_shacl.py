"""
SHACL validation for CSVW-SAFE metadata.

This module validates CSVW-SAFE metadata files against a SHACL schema
using the pySHACL engine. The metadata is expected to be in JSON-LD
format and the SHACL shapes in Turtle format.

Requires
--------
pyshacl
rdflib
"""

import argparse
import sys
from pathlib import Path

from pyshacl import validate as shacl_validate
from rdflib import Graph


def validate_metadata_shacl(metadata_file: Path, shacl_file: Path) -> tuple[bool, str]:
    """
    Validate CSVW-SAFE metadata against a SHACL schema.

    Parameters
    ----------
    metadata_file : Path
        Path to the metadata file in JSON-LD format.
    shacl_file : Path
        Path to the SHACL shapes file in Turtle format.

    Returns
    -------
    Tuple[bool, str]
        A tuple containing:
        - bool : Whether the metadata conforms to the SHACL schema.
        - str : Textual validation report produced by pySHACL.
    """
    data_graph = Graph()
    data_graph.parse(metadata_file, format="json-ld")

    shacl_graph = Graph()
    shacl_graph.parse(shacl_file, format="turtle")

    conforms, _, results_text = shacl_validate(
        data_graph,
        shacl_graph=shacl_graph,
        inference="rdfs",
        abort_on_first=False,
        meta_shacl=False,
        debug=False,
    )

    return conforms, results_text


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
    parser.add_argument("shacl_file", type=str, help="SHACL TTL file")
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    shacl_path = Path(args.shacl_file)

    if not metadata_path.exists():
        print(f"Metadata file not found: {metadata_path}")  # noqa: T201
        sys.exit(1)
    if not shacl_path.exists():
        print(f"SHACL file not found: {shacl_path}")  # noqa: T201
        sys.exit(1)

    try:
        conforms, results_text = validate_metadata_shacl(metadata_path, shacl_path)
    except ImportError:
        print("pySHACL not installed. Please install it with `pip install pyshacl`")  # noqa: T201
        sys.exit(1)

    if conforms:
        print("SHACL validation SUCCESSFUL")  # noqa: T201
    else:
        print("SHACL validation FAILED")  # noqa: T201
        print(results_text)  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    main()
