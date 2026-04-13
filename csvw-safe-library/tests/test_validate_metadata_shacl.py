import json

import pytest

from csvw_safe import constants as c
from csvw_safe.datatypes import DataTypes
from csvw_safe.validate_metadata_shacl import validate_metadata_shacl


@pytest.fixture(scope="session")
def shacl_path():
    return "../csvw-safe-constraints.ttl"


@pytest.fixture(scope="session")
def metadata_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("metadata")


def test_validate_metadata_minimal(shacl_path, metadata_dir):
    """Test minimal table metadata."""
    metadata = {
        c.PRIVACY_UNIT: "person",
        c.MAX_CONTRIB: 5,
        c.MAX_LENGTH: 10,
        c.PUBLIC_LENGTH: 10,
        c.TABLE_SCHEMA: {
            c.COL_LIST: [
                {
                    "@type": c.COL_TYPE,
                    c.COL_NAME: "col1",
                    c.DATATYPE: DataTypes.INTEGER,
                    c.REQUIRED: True,
                    c.PRIVACY_ID: False,
                    c.NULL_PROP: 0.0,
                }
            ]
        },
    }
    metadata_dir
    path = metadata_dir / "test.json-ld"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f)
    validate_metadata_shacl(path, shacl_path)
