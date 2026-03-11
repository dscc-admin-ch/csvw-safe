import pandas as pd
import pytest

from csvw_safe import constants as C
from csvw_safe.make_metadata_from_data import build_partitions, make_metadata_from_data
from csvw_safe.metadata_structure import (
    MultiColumnPartition,
    Predicate,
    SingleColumnPartition,
)


@pytest.fixture
def simple_df():
    return pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 3],
            "color": ["red", "blue", "red", "blue", "red"],
            "value": [10, 20, 30, 40, 50],
            "timestamp": pd.date_range("2025-01-01", periods=5),
        }
    )


def test_categorical_partition(simple_df):
    column_specs = [{"name": "color", "kind": "categorical", "is_datetime": False}]
    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        SingleColumnPartition(
            predicate=Predicate(partition_value="blue"),
            max_length=2,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        SingleColumnPartition(
            predicate=Predicate(partition_value="red"),
            max_length=3,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_numeric_partition(simple_df):
    column_specs = [
        {"name": "value", "kind": "continuous", "bins": [0, 25, 50, 60], "is_datetime": False}
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        SingleColumnPartition(
            predicate=Predicate(lower_bound=0.0, upper_bound=25.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        SingleColumnPartition(
            predicate=Predicate(lower_bound=25.0, upper_bound=50.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        SingleColumnPartition(
            predicate=Predicate(lower_bound=50.0, upper_bound=60.0),
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=1,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_mixed_partitions(simple_df):
    column_specs = [
        {"name": "color", "kind": "categorical", "is_datetime": False},
        {"name": "value", "kind": "continuous", "bins": [0, 25, 50, 60], "is_datetime": False},
    ]

    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        MultiColumnPartition(
            predicate={
                "color": Predicate(partition_value="blue"),
                "value": Predicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": Predicate(partition_value="blue"),
                "value": Predicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": Predicate(partition_value="red"),
                "value": Predicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": Predicate(partition_value="red"),
                "value": Predicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": Predicate(partition_value="red"),
                "value": Predicate(lower_bound=50.0, upper_bound=60.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=1,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_column_level_partitions(simple_df):

    metadata = make_metadata_from_data(
        df=simple_df,
        privacy_unit="user_id",
        fine_contributions_level={"color": "column"},
    )
    color_column = next(c for c in metadata[C.TABLE_SCHEMA][C.COL_LIST] if c[C.COL_NAME] == "color")

    assert color_column[C.PUBLIC_PARTITIONS] == ["blue", "red"]
    assert color_column[C.MAX_NUM_PARTITIONS] == 2
    assert color_column[C.MAX_LENGTH] == 3
    assert color_column[C.MAX_GROUPS] == 1
    assert color_column[C.MAX_CONTRIB] == 2


def test_partition_level_partitions(simple_df):

    metadata = make_metadata_from_data(
        df=simple_df,
        privacy_unit="user_id",
        fine_contributions_level={"color": "partition"},
    )

    color_column = next(c for c in metadata[C.TABLE_SCHEMA][C.COL_LIST] if c[C.COL_NAME] == "color")
    print(color_column)

    assert isinstance(color_column[C.PUBLIC_PARTITIONS], list)
    assert isinstance(color_column[C.PUBLIC_PARTITIONS][0], dict)

    assert color_column[C.MAX_NUM_PARTITIONS] == 2
