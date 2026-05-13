import numpy as np
import pandas as pd
import pytest

from csvw_eo import constants as c
from csvw_eo.make_metadata_from_data import make_metadata_from_data


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
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id", with_dependencies=False)

    assert metadata["@type"] == c.TABLE_TYPE
    assert metadata[c.PRIVACY_UNIT] == "user_id"
    assert metadata[c.MAX_LENGTH] == len(small_df)
    assert metadata[c.PUBLIC_LENGTH] == len(small_df)
    assert c.ADD_INFO not in metadata

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    assert len(columns) == len(small_df.columns)

    privacy_col = next(col for col in columns if col[c.COL_NAME] == "user_id")
    assert privacy_col[c.PRIVACY_ID] is True

    color_col = next(col for col in columns if col[c.COL_NAME] == "color")
    assert c.KEY_VALUES not in color_col


def test_basic_metadata_table_with_keys(small_df):
    metadata = make_metadata_from_data(
        small_df,
        privacy_unit="user_id",
        with_dependencies=False,
        default_contributions_level="table_with_keys",
    )

    assert metadata["@type"] == c.TABLE_TYPE
    assert metadata[c.PRIVACY_UNIT] == "user_id"
    assert metadata[c.MAX_LENGTH] == len(small_df)
    assert metadata[c.PUBLIC_LENGTH] == len(small_df)

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    assert len(columns) == len(small_df.columns)

    privacy_col = next(col for col in columns if col[c.COL_NAME] == "user_id")
    assert privacy_col[c.PRIVACY_ID] is True

    color_col = next(col for col in columns if col[c.COL_NAME] == "color")
    assert color_col[c.KEY_VALUES] == ["blue", "red"]


def test_basic_metadata_big(big_df):
    metadata = make_metadata_from_data(big_df, privacy_unit="user_id")
    assert metadata[c.MAX_LENGTH] == len(big_df)
    assert metadata[c.PUBLIC_LENGTH] == len(big_df)


def test_missing_privacy_unit(small_df):
    with pytest.raises(ValueError):
        make_metadata_from_data(small_df, privacy_unit="missing_column")


def test_nullable_proportion_small():
    df = pd.DataFrame({"user_id": [1, 1, 2, 2, 3], "nullable": [1, None, None, 2, 3]})
    metadata = make_metadata_from_data(df, privacy_unit="user_id")

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    nullable_col = next(col for col in columns if col[c.COL_NAME] == "nullable")

    assert nullable_col[c.NULL_PROP] == 0.4
    assert nullable_col[c.REQUIRED] is False


def test_categorical_partitions_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id", default_contributions_level="column")

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    color_col = next(col for col in columns if col[c.COL_NAME] == "color")

    assert c.KEY_VALUES in color_col
    assert color_col[c.MAX_NUM_PARTITIONS] == 2

    # Partition values at column-level are just values
    partition_values = set(color_col[c.KEY_VALUES])
    assert partition_values == {"red", "blue"}


def test_numeric_partitions_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="column",
    )

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    value_col = next(col for col in columns if col[c.COL_NAME] == "value")

    assert c.PUBLIC_PARTITIONS in value_col
    assert value_col[c.MAX_NUM_PARTITIONS] == 4

    partitions = value_col[c.PUBLIC_PARTITIONS]
    expected = [
        {c.LOWER_BOUND: 0.0, c.UPPER_BOUND: 25.0},
        {c.LOWER_BOUND: 25.0, c.UPPER_BOUND: 50.0},
        {c.LOWER_BOUND: 50.0, c.UPPER_BOUND: 75.0},
        {c.LOWER_BOUND: 75.0, c.UPPER_BOUND: 100.0},
    ]
    for p, e in zip(partitions, expected):
        assert p[c.PREDICATE][c.LOWER_BOUND] == e[c.LOWER_BOUND]
        assert p[c.PREDICATE][c.UPPER_BOUND] == e[c.UPPER_BOUND]


def test_partition_contribution_level_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="partition",
    )

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    value_col = next(col for col in columns if col[c.COL_NAME] == "value")

    partitions = value_col[c.PUBLIC_PARTITIONS]

    assert isinstance(partitions, list)
    assert len(partitions) > 0

    first_partition = partitions[0]
    expected_first_partition = {
        "@type": c.PARTITION,
        c.PREDICATE: {
            c.LOWER_BOUND: 0.0,
            c.UPPER_BOUND: 25.0,
        },
        c.MAX_LENGTH: 15,
        c.MAX_GROUPS: 3,
        c.MAX_CONTRIB: 1,
    }
    assert first_partition == expected_first_partition


def test_column_groups_big_partition_level(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        column_groups=[["color", "value"]],
        default_contributions_level="partition",
    )

    assert c.ADD_INFO in metadata

    groups = metadata[c.ADD_INFO]
    assert len(groups) == 1

    group = groups[0]
    assert group["@type"] == c.COLUMN_GROUP
    assert group[c.COLUMNS_IN_GROUP] == ["color", "value"]
    assert c.PUBLIC_PARTITIONS in group


def test_column_groups_big_column_level(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        column_groups=[["color", "value"]],
        default_contributions_level="column",
    )

    assert c.ADD_INFO in metadata

    groups = metadata[c.ADD_INFO]
    assert len(groups) == 1

    group = groups[0]
    assert group["@type"] == c.COLUMN_GROUP
    assert group[c.COLUMNS_IN_GROUP] == ["color", "value"]
    assert c.PUBLIC_PARTITIONS not in group
    assert group[c.MAX_NUM_PARTITIONS] == 12


def test_fine_contribution_override_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="table",
        fine_contributions_level={"value": "column"},
    )

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    value_col = next(col for col in columns if col[c.COL_NAME] == "value")

    assert c.PUBLIC_PARTITIONS in value_col


def test_numeric_bounds_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id")

    columns = metadata[c.TABLE_SCHEMA][c.COL_LIST]
    value_col = next(col for col in columns if col[c.COL_NAME] == "value")

    assert value_col[c.MINIMUM] == 10
    assert value_col[c.MAXIMUM] == 50
