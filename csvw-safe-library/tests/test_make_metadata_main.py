import pandas as pd
import numpy as np
import pytest
from csvw_safe.make_metadata_from_data import make_metadata_from_data


@pytest.fixture
def small_df():
    """
    Small dataset → many columns interpreted as categorical.
    """
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
    """
    Big dataset → ensures continuous logic definitely triggers.
    """
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

    assert metadata["@type"] == "csvw:Table"
    assert metadata["csvw-safe:public.privacyUnit"] == "user_id"
    assert metadata["csvw-safe:bounds.maxLength"] == len(small_df)
    assert metadata["csvw-safe:public.length"] == len(small_df)

    columns = metadata["csvw:tableSchema"]["columns"]
    assert len(columns) == len(small_df.columns)

    privacy_col = next(c for c in columns if c["name"] == "user_id")
    assert privacy_col["csvw-safe:public.privacyId"] is True


def test_basic_metadata_big(big_df):

    metadata = make_metadata_from_data(big_df, privacy_unit="user_id")

    assert metadata["csvw-safe:bounds.maxLength"] == len(big_df)
    assert metadata["csvw-safe:public.length"] == len(big_df)


def test_missing_privacy_unit(small_df):

    with pytest.raises(ValueError):
        make_metadata_from_data(small_df, privacy_unit="missing_column")


def test_nullable_proportion_small():

    df = pd.DataFrame({"user_id": [1, 1, 2, 2, 3], "nullable": [1, None, None, 2, 3]})

    metadata = make_metadata_from_data(df, privacy_unit="user_id")

    columns = metadata["csvw:tableSchema"]["columns"]
    nullable_col = next(c for c in columns if c["name"] == "nullable")

    assert nullable_col["csvw-safe:synth.nullableProportion"] == 0.4
    assert nullable_col["required"] is False


def test_categorical_partitions_small(small_df):

    metadata = make_metadata_from_data(
        small_df, privacy_unit="user_id", default_contributions_level="column"
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    color_col = next(c for c in columns if c["name"] == "color")

    assert "csvw-safe:public.partitions" in color_col
    assert color_col["csvw-safe:public.maxNumPartitions"] == 2

    assert set(color_col["csvw-safe:public.partitions"]) == {"red", "blue"}


def test_numeric_partitions_big(big_df):

    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="column",
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    assert "csvw-safe:public.partitions" in value_col
    assert value_col["csvw-safe:public.maxNumPartitions"] == 4

    partitions = value_col["csvw-safe:public.partitions"]

    assert partitions == [
        {"lowerBound": 0.0, "upperBound": 25.0},
        {"lowerBound": 25.0, "upperBound": 50.0},
        {"lowerBound": 50.0, "upperBound": 75.0},
        {"lowerBound": 75.0, "upperBound": 100.0},
    ]


def test_partition_contribution_level_big(big_df):

    metadata = make_metadata_from_data(
        big_df,
        privacy_unit="user_id",
        continuous_partitions={"value": [0, 25, 50, 75, 100]},
        default_contributions_level="partition",
    )

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    partitions = value_col["csvw-safe:public.partitions"]

    assert isinstance(partitions, list)
    assert "@type" in partitions[0]
    assert "csvw-safe:bounds.maxLength" in partitions[0]


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

    assert group["@type"] == "csvw-safe:ColumnGroup"
    assert group["csvw-safe:columns"] == ["color", "value"]
    assert "csvw-safe:public.partitions" in group


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

    assert "csvw-safe:public.partitions" in value_col


def test_numeric_bounds_small(small_df):

    metadata = make_metadata_from_data(small_df, privacy_unit="user_id")

    columns = metadata["csvw:tableSchema"]["columns"]
    value_col = next(c for c in columns if c["name"] == "value")

    assert value_col["minimum"] == 10
    assert value_col["maximum"] == 50
