"""
SHACL validation for CSVW-SAFE metadata.

Requires pySHACL.
"""

import sys
from pathlib import Path
from rdflib import Graph
from pyshacl import validate as shacl_validate


def validate_metadata_shacl(metadata_file: Path, shacl_file: Path):
    data_graph = Graph()
    data_graph.parse(metadata_file, format="json-ld")

    shacl_graph = Graph()
    shacl_graph.parse(shacl_file, format="turtle")

    conforms, results_graph, results_text = shacl_validate(
        data_graph,
        shacl_graph=shacl_graph,
        inference='rdfs',
        abort_on_first=False,
        meta_shacl=False,
        debug=False
    )

    return conforms, results_text

def main():
    import argparse
    parser = argparse.ArgumentParser(description="SHACL validation for CSVW-SAFE metadata")
    parser.add_argument("metadata_file", type=str)
    parser.add_argument("shacl_file", type=str, help="SHACL TTL file")
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    shacl_path = Path(args.shacl_file)

    if not metadata_path.exists():
        print(f"Metadata file not found: {metadata_path}")
        sys.exit(1)
    if not shacl_path.exists():
        print(f"SHACL file not found: {shacl_path}")
        sys.exit(1)

    try:
        conforms, results_text = validate_metadata_shacl(metadata_path, shacl_path)
    except ImportError:
        print("pySHACL not installed. Please install it with `pip install pyshacl`")
        sys.exit(1)

    if conforms:
        print("SHACL validation SUCCESS ✅ Metadata satisfies SHACL constraints")
    else:
        print("SHACL validation FAILED ❌ Metadata violates SHACL constraints")
        print(results_text)
        sys.exit(1)

if __name__ == "__main__":
    main()