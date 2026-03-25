import pytest
import yaml

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    MAX_CONTRIB,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PRIVACY_ID,
    REQUIRED,
)
from csvw_safe.csvw_to_smartnoise_sql import csvw_to_smartnoise_sql


def mock_csvw_metadata():
    """Return a small CSVW-SAFE JSON metadata for testing."""
    return {
        MAX_CONTRIB: 1,  # required by csvw_to_smartnoise_sql
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
            {COL_NAME: "signup_date", DATATYPE: "dateTime", NULL_PROP: 0.1},
        ],
    }


def test_csvw_to_smartnoise_sql_basic():
    """Test conversion of CSVW metadata to SmartNoise SQL table metadata."""
    csvw_meta = mock_csvw_metadata()
    schema_name = "TestSchema"
    table_name = "TestTable"
    row_privacy = True

    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name=schema_name,
        table_name=table_name,
        row_privacy=row_privacy,
    )

    # Check top-level keys
    assert "" in snsql_meta
    assert schema_name in snsql_meta[""]
    assert table_name in snsql_meta[""][schema_name]

    table_meta = snsql_meta[""][schema_name][table_name]

    # Check table-level properties
    assert table_meta["max_ids"] == 1
    assert table_meta["row_privacy"] == row_privacy
    assert table_meta["sample_max_ids"] is True
    assert table_meta["censor_dims"] is True
    assert table_meta["clamp_counts"] is False
    assert table_meta["clamp_columns"] is True
    assert table_meta["use_dpsu"] is False

    # Check columns
    user_col = table_meta["user_id"]
    assert user_col["type"] == "int"
    assert user_col["private_id"] is True
    assert user_col["lower"] == 1
    assert user_col["upper"] == 100
    assert user_col["nullable"] is False

    age_col = table_meta["age"]
    assert age_col["type"] == "int"
    assert "private_id" not in age_col
    assert age_col["lower"] == 0
    assert age_col["upper"] == 120
    assert age_col["nullable"] is False

    signup_col = table_meta["signup_date"]
    assert signup_col["type"] == "datetime"
    assert signup_col["nullable"] is True


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
        COL_LIST: [
            {COL_NAME: "col1", DATATYPE: "integer", NULL_PROP: 0.5},
            {COL_NAME: "col2", DATATYPE: "string", NULL_PROP: 0.0},
        ],
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
        COL_LIST: [
            {COL_NAME: "col1", DATATYPE: "double", NULL_PROP: 0.5},
            {COL_NAME: "col2", DATATYPE: "boolean", NULL_PROP: 0.0},
        ],
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
