import pandas as pd
import numpy as np
import pytest
from csvw_safe.make_metadata_from_data import make_metadata_from_data
import csvw_safe.constants as C


@pytest.fixture
def small_df():
    return pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 3],
            "color": ["red", "blue", "red", "red", "blue"],
            "value": [10, 20, 30, 40, 50],
            "timestamp": pd.date_range("2025-01-01", periods=5),
        }
    )


@pytest.fixture
def big_df():
    n = 60
    return pd.DataFrame(
        {
            "user_id": np.repeat(np.arange(1, 21), 3),
            "color": ["red", "blue", "green"] * 20,
            "value": np.linspace(0, 100, n),
            "timestamp": pd.date_range("2025-01-01", periods=n),
        }
    )


def test_basic_metadata_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id")
    print(metadata)
    assert metadata["@type"] == "csvw:Table"
    assert metadata[C.PRIVACY_UNIT] == "user_id"
    assert metadata[C.MAX_LENGTH] == len(small_df)
    assert metadata[C.PUBLIC_LENGTH] == len(small_df)

    columns = metadata["csvw:tableSchema"]["columns"]
    assert len(columns) == len(small_df.columns)

    privacy_col = next(c for c in columns if c["name"] == "user_id")
    assert privacy_col[C.PRIVACY_ID] is True


def test_basic_metadata_big(big_df):
    metadata = make_metadata_from_data(big_df, privacy_unit="user_id")
    assert metadata[C.MAX_LENGTH] == len(big_df)
    assert metadata[C.PUBLIC_LENGTH] == len(big_df)


def test_missing_privacy_unit(small_df):
    with pytest.raises(ValueError):
        make_metadata_from_data(small_df, privacy_unit="missing_column")


def test_nullable_proportion_small():
    df = pd.DataFrame({"user_id": [1, 1, 2, 2, 3], "nullable": [1, None, None, 2, 3]})
    metadata = make_metadata_from_data(df, privacy_unit="user_id")

    columns = metadata["csvw:tableSchema"]["columns"]
    nullable_col = next(c for c in columns if c["name"] == "nullable")

    assert nullable_col[C.NULL_PROP] == 0.4
    assert nullable_col["required"] is False


def test_categorical_partitions_small(small_df):
    metadata = make_metadata_from_data(
        small_df, privacy_unit="user_id", default_contributions_level="column"
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    color_col = next(c for c in columns if c["name"] == "color")

    assert C.PUBLIC_PARTITIONS in color_col
    assert color_col[C.MAX_NUM_PARTITIONS] == 2

    # Partition values at column-level are just values
    partition_values = set(color_col[C.PUBLIC_PARTITIONS])
    assert partition_values == {"red", "blue"}


def test_numeric_partitions_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="column",
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    assert C.PUBLIC_PARTITIONS in value_col
    assert value_col[C.MAX_NUM_PARTITIONS] == 4

    partitions = value_col[C.PUBLIC_PARTITIONS]
    expected = [
        {C.LOWER_BOUND: 0.0, C.UPPER_BOUND: 25.0},
        {C.LOWER_BOUND: 25.0, C.UPPER_BOUND: 50.0},
        {C.LOWER_BOUND: 50.0, C.UPPER_BOUND: 75.0},
        {C.LOWER_BOUND: 75.0, C.UPPER_BOUND: 100.0},
    ]
    for p, e in zip(partitions, expected):
        assert p[C.PREDICATE][C.LOWER_BOUND] == e[C.LOWER_BOUND]
        assert p[C.PREDICATE][C.UPPER_BOUND] == e[C.UPPER_BOUND]


def test_partition_contribution_level_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="partition",
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    partitions = value_col[C.PUBLIC_PARTITIONS]

    assert isinstance(partitions, list)
    assert "@type" in partitions[0]  # now each partition has "@type"
    assert C.MAX_LENGTH in partitions[0]


def test_column_groups_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        column_groups=[["color", "value"]],
        default_contributions_level="partition",
    )

    assert "csvw-safe:additionalInformation" in metadata

    groups = metadata["csvw-safe:additionalInformation"]
    assert len(groups) == 1

    group = groups[0]
    assert group["@type"] == C.COLUMN_GROUP
    assert group["csvw-safe:columns"] == ["color", "value"]
    assert C.PUBLIC_PARTITIONS in group


def test_fine_contribution_override_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="table",
        fine_contributions_level={"value": "column"},
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    assert C.PUBLIC_PARTITIONS in value_col


def test_numeric_bounds_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id")

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    assert value_col["minimum"] == 10
    assert value_col["maximum"] == 50
