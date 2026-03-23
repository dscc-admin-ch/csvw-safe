import yaml

from csvw_safe.csvw_to_smartnoise_sql import csvw_to_smartnoise_sql


def mock_csvw_metadata():
    """Return a small CSVW-SAFE JSON metadata for testing."""
    return {
        "columns": [
            {
                "name": "user_id",
                "datatype": "integer",
                "privacy_id": True,
                "nullable_proportion": 0.0,
                "minimum": 1,
                "maximum": 100,
            },
            {
                "name": "age",
                "datatype": "integer",
                "nullable_proportion": 0.0,
                "minimum": 0,
                "maximum": 120,
            },
            {"name": "signup_date", "datatype": "dateTime", "nullable_proportion": 0.1},
        ]
    }


def test_csvw_to_smartnoise_sql_basic():
    """Test conversion of CSVW metadata to SmartNoise SQL table metadata."""
    csvw_meta = mock_csvw_metadata()
    schema_name = "TestSchema"
    table_name = "TestTable"
    privacy_unit = "user_id"
    max_ids = 1
    row_privacy = True

    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta, schema_name, table_name, privacy_unit, max_ids, row_privacy
    )

    # Check top-level keys
    assert "" in snsql_meta
    assert schema_name in snsql_meta[""]
    assert table_name in snsql_meta[""][schema_name]

    table_meta = snsql_meta[""][schema_name][table_name]

    # Check table-level properties
    assert table_meta["max_ids"] == max_ids
    assert table_meta["row_privacy"] == row_privacy

    # Check columns
    user_col = table_meta["user_id"]
    assert user_col["type"] == "int"
    assert user_col["private_id"] is True
    assert user_col["lower"] == 1
    assert user_col["upper"] == 100

    age_col = table_meta["age"]
    assert age_col["type"] == "int"
    assert "private_id" not in age_col
    assert age_col["lower"] == 0
    assert age_col["upper"] == 120

    signup_col = table_meta["signup_date"]
    print("*********")
    print(signup_col)
    assert signup_col["type"] == "datetime"


def test_yaml_output(tmp_path):
    """Test writing the SmartNoise metadata to a YAML file."""
    csvw_meta = mock_csvw_metadata()
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta, "Schema", "Table", "user_id", max_ids=1, row_privacy=False
    )

    out_file = tmp_path / "metadata.yaml"
    with open(out_file, "w") as f:
        yaml.safe_dump(snsql_meta, f)

    # Load back the YAML and verify
    with open(out_file) as f:
        loaded = yaml.safe_load(f)

    assert "" in loaded
    assert "Schema" in loaded[""]
    assert "Table" in loaded[""]["Schema"]
    table_meta = loaded[""]["Schema"]["Table"]
    assert table_meta["user_id"]["type"] == "int"
    assert table_meta["age"]["upper"] == 120


def test_column_nullable_handling():
    """Test nullable_proportion is converted correctly (nullable flag)."""
    csvw_meta = {
        "columns": [
            {"name": "col1", "datatype": "integer", "nullable_proportion": 0.5},
            {"name": "col2", "datatype": "string", "nullable_proportion": 0.0},
        ]
    }
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta, "Schema", "Table", privacy_unit="", max_ids=1, row_privacy=False
    )
    table_meta = snsql_meta[""]["Schema"]["Table"]

    assert table_meta["col1"]["nullable"] is True
    assert table_meta["col2"]["nullable"] is False
