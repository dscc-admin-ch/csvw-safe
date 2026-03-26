import json
import pytest

from csvw_safe import constants as C
from csvw_safe.validate_metadata_shacl import validate_metadata_shacl
from csvw_safe.datatypes import DataTypes


@pytest.fixture(scope="session")
def shacl_path():
    return "../csvw-safe-constraints.ttl"


@pytest.fixture(scope="session")
def metadata_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("metadata")


def test_validate_metadata_minimal(shacl_path, metadata_dir):
    """Test minimal table metadata."""
    metadata = {
        C.PRIVACY_UNIT: "person",
        C.MAX_CONTRIB: 5,
        C.MAX_LENGTH: 10,
        C.PUBLIC_LENGTH: 10,
        C.TABLE_SCHEMA: {
            C.COL_LIST: [
                {
                    "@type": C.COL_TYPE,
                    C.COL_NAME: "col1",
                    C.DATATYPE: DataTypes.INTEGER,
                    C.REQUIRED: True,
                    C.PRIVACY_ID: False,
                    C.NULL_PROP: 0.0,
                }
            ]
        },
    }
    metadata_dir
    path = metadata_dir / "test.json-ld"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f)
    validate_metadata_shacl(path, shacl_path)
