import pytest
import yaml

from csvw_eo.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    MAX_CONTRIB,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PRIVACY_ID,
    REQUIRED,
    TABLE_SCHEMA,
)
from csvw_eo.csvw_to_smartnoise_sql import csvw_to_smartnoise_sql


def mock_csvw_metadata():
    """Return a small CSVW-EO JSON metadata for testing."""
    return {
        MAX_CONTRIB: 1,  # required by csvw_to_smartnoise_sql
        TABLE_SCHEMA: {
            COL_LIST: [
                {
                    COL_NAME: "user_id",
                    DATATYPE: "integer",
                    PRIVACY_ID: True,
                    NULL_PROP: 0.0,
                    MINIMUM: 1,
                    MAXIMUM: 100,
                },
                {
                    COL_NAME: "age",
                    DATATYPE: "integer",
                    REQUIRED: True,
                    NULL_PROP: 0.0,
                    MINIMUM: 0,
                    MAXIMUM: 120,
                },
                {
                    COL_NAME: "signup_date",
                    DATATYPE: "dateTime",
                    NULL_PROP: 0.1,
                    MINIMUM: "2016/04/27",
                    MAXIMUM: "2026/04/17",
                },
            ],
        },
    }


def test_csvw_to_smartnoise_sql_basic():
    """Test conversion of CSVW metadata to SmartNoise SQL table metadata."""
    csvw_meta = mock_csvw_metadata()
    schema_name = "TestSchema"
    table_name = "TestTable"

    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name=schema_name,
        table_name=table_name,
    )
    print(snsql_meta)

    # Check top-level keys
    assert "" in snsql_meta
    assert schema_name in snsql_meta[""]
    assert table_name in snsql_meta[""][schema_name]

    table_meta = snsql_meta[""][schema_name][table_name]

    expected = {
        "max_ids": 1,
        "row_privacy": False,
        "user_id": {
            "name": "user_id",
            "type": "int",
            "nullable": False,
            "private_id": True,
        },
        "age": {
            "name": "age",
            "type": "int",
            "nullable": False,
            "lower": 0,
            "upper": 120,
        },
        "signup_date": {
            "name": "signup_date",
            "type": "datetime",
            "nullable": True,
        },
    }

    assert table_meta == expected


def test_yaml_output(tmp_path):
    """Test writing the SmartNoise metadata to a YAML file."""
    csvw_meta = mock_csvw_metadata()
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name="Schema",
        table_name="Table",
    )

    out_file = tmp_path / "metadata.yaml"
    with open(out_file, "w") as f:
        yaml.safe_dump(snsql_meta, f)

    # Load back the YAML and verify
    with open(out_file) as f:
        loaded = yaml.safe_load(f)

    table_meta = loaded[""]["Schema"]["Table"]
    assert table_meta["user_id"]["type"] == "int"
    assert table_meta["age"]["upper"] == 120
    assert table_meta["signup_date"]["nullable"] is True


def test_column_nullable_handling():
    """Test nullable_proportion is converted correctly to nullable flag."""
    csvw_meta = {
        MAX_CONTRIB: 1,
        TABLE_SCHEMA: {
            COL_LIST: [
                {COL_NAME: "col1", DATATYPE: "integer", NULL_PROP: 0.5},
                {COL_NAME: "col2", DATATYPE: "string", NULL_PROP: 0.0},
            ],
        },
    }
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name="Schema",
        table_name="Table",
    )
    table_meta = snsql_meta[""]["Schema"]["Table"]

    assert table_meta["col1"]["nullable"] is True
    assert table_meta["col2"]["nullable"] is False


def test_float_and_boolean():
    """Test nullable_proportion is converted correctly to nullable flag."""
    csvw_meta = {
        MAX_CONTRIB: 1,
        TABLE_SCHEMA: {
            COL_LIST: [
                {COL_NAME: "col1", DATATYPE: "double", NULL_PROP: 0.5},
                {COL_NAME: "col2", DATATYPE: "boolean", NULL_PROP: 0.0},
            ],
        },
    }
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name="Schema",
        table_name="Table",
    )
    table_meta = snsql_meta[""]["Schema"]["Table"]

    assert table_meta["col1"]["type"] == "float"
    assert table_meta["col2"]["type"] == "boolean"


def test_missing_max_contributions_raises():
    """Test that missing max_contributions raises ValueError."""
    csvw_meta = {"columns": [{COL_NAME: "col1", DATATYPE: "integer"}]}
    with pytest.raises(ValueError):
        csvw_to_smartnoise_sql(
            csvw_meta=csvw_meta,
            schema_name="Schema",
            table_name="Table",
        )
