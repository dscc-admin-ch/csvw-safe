import numpy as np
import pandas as pd
import pytest

import csvw_safe.constants as C
from csvw_safe.make_metadata_from_data import make_metadata_from_data


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

    assert metadata["@type"] == C.TABLE_TYPE
    assert metadata[C.PRIVACY_UNIT] == "user_id"
    assert metadata[C.MAX_LENGTH] == len(small_df)
    assert metadata[C.PUBLIC_LENGTH] == len(small_df)
    assert C.ADD_INFO not in metadata

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    assert len(columns) == len(small_df.columns)

    privacy_col = next(c for c in columns if c[C.COL_NAME] == "user_id")
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

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    nullable_col = next(c for c in columns if c[C.COL_NAME] == "nullable")

    assert nullable_col[C.NULL_PROP] == 0.4
    assert nullable_col[C.REQUIRED] is False


def test_categorical_partitions_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id", default_contributions_level="column")

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    color_col = next(c for c in columns if c[C.COL_NAME] == "color")

    assert C.KEY_VALUES in color_col
    assert color_col[C.MAX_NUM_PARTITIONS] == 2

    # Partition values at column-level are just values
    partition_values = set(color_col[C.KEY_VALUES])
    assert partition_values == {"red", "blue"}


def test_numeric_partitions_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="column",
    )

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    value_col = next(c for c in columns if c[C.COL_NAME] == "value")

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

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    value_col = next(c for c in columns if c[C.COL_NAME] == "value")

    partitions = value_col[C.PUBLIC_PARTITIONS]

    assert isinstance(partitions, list)
    assert len(partitions) > 0

    first_partition = partitions[0]
    expected_first_partition = {
        "@type": C.PARTITION,
        C.PREDICATE: {
            C.LOWER_BOUND: 0.0,
            C.UPPER_BOUND: 25.0,
        },
        C.MAX_LENGTH: 15,
        C.MAX_GROUPS: 3,
        C.MAX_CONTRIB: 1,
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

    assert C.ADD_INFO in metadata

    groups = metadata[C.ADD_INFO]
    assert len(groups) == 1

    group = groups[0]
    assert group["@type"] == C.COLUMN_GROUP
    assert group[C.COLUMNS_IN_GROUP] == ["color", "value"]
    assert C.PUBLIC_PARTITIONS in group


def test_column_groups_big_column_level(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        column_groups=[["color", "value"]],
        default_contributions_level="column",
    )

    assert C.ADD_INFO in metadata

    groups = metadata[C.ADD_INFO]
    assert len(groups) == 1

    group = groups[0]
    assert group["@type"] == C.COLUMN_GROUP
    assert group[C.COLUMNS_IN_GROUP] == ["color", "value"]
    assert C.PUBLIC_PARTITIONS not in group
    assert group[C.MAX_NUM_PARTITIONS] == 12


def test_fine_contribution_override_big(big_df):
    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="table",
        fine_contributions_level={"value": "column"},
    )

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    value_col = next(c for c in columns if c[C.COL_NAME] == "value")

    assert C.PUBLIC_PARTITIONS in value_col


def test_numeric_bounds_small(small_df):
    metadata = make_metadata_from_data(small_df, privacy_unit="user_id")

    columns = metadata[C.TABLE_SCHEMA][C.COL_LIST]
    value_col = next(c for c in columns if c[C.COL_NAME] == "value")

    assert value_col[C.MINIMUM] == 10
    assert value_col[C.MAXIMUM] == 50
