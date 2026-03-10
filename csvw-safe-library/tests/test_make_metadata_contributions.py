import pandas as pd
import pytest

from csvw_safe import constants as C
from csvw_safe import metadata_structure as S
from csvw_safe.make_metadata_from_data import (
    ContributionLevel,
    attach_partitions_to_column,
    build_partitions,
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
        S.SingleColumnPartition(
            predicate=S.Predicate(partition_value="blue"),
            max_length=2,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        S.SingleColumnPartition(
            predicate=S.Predicate(partition_value="red"),
            max_length=3,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_numeric_partition(simple_df):
    column_specs = [{"name": "value", "kind": "numeric", "bins": [0, 25, 50, 60], "is_datetime": False}]
    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        S.SingleColumnPartition(
            predicate=S.Predicate(lower_bound=0.0, upper_bound=25.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        S.SingleColumnPartition(
            predicate=S.Predicate(lower_bound=25.0, upper_bound=50.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        S.SingleColumnPartition(
            predicate=S.Predicate(lower_bound=50.0, upper_bound=60.0),
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=1,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_mixed_partitions(simple_df):
    column_specs = [
        {"name": "color", "kind": "categorical", "is_datetime": False},
        {"name": "value", "kind": "numeric", "bins": [0, 25, 50, 60], "is_datetime": False},
    ]

    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        S.MultiColumnPartition(
            predicate={
                "color": S.Predicate(partition_value="blue"),
                "value": S.Predicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        S.MultiColumnPartition(
            predicate={
                "color": S.Predicate(partition_value="blue"),
                "value": S.Predicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        S.MultiColumnPartition(
            predicate={
                "color": S.Predicate(partition_value="red"),
                "value": S.Predicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        S.MultiColumnPartition(
            predicate={
                "color": S.Predicate(partition_value="red"),
                "value": S.Predicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        S.MultiColumnPartition(
            predicate={
                "color": S.Predicate(partition_value="red"),
                "value": S.Predicate(lower_bound=50.0, upper_bound=60.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=1,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


@pytest.fixture
def partitions_categorical():
    return [
        S.SingleColumnPartition(
            predicate=p[C.PREDICATE],
            max_length=p[C.MAX_LENGTH],
            max_groups_per_unit=p[C.MAX_GROUPS],
            max_contributions=p[C.MAX_CONTRIB],
        )
        for p in [
            {
                C.PREDICATE: S.Predicate(partition_value="Red"),
                C.MAX_LENGTH: 3,
                C.MAX_GROUPS: 2,
                C.MAX_CONTRIB: 1,
            },
            {
                C.PREDICATE: S.Predicate(partition_value="Blue"),
                C.MAX_LENGTH: 5,
                C.MAX_GROUPS: 1,
                C.MAX_CONTRIB: 2,
            },
        ]
    ]


def test_attach_partitions_column_level(partitions_categorical):
    column_meta = S.ColumnMetadata(
        name="value", datatype="integer", required=True, privacy_id=False, nullable_proportion=0
    )
    col_contrib_level = ContributionLevel.COLUMN
    result = attach_partitions_to_column(column_meta, partitions_categorical, col_contrib_level)

    expected_result = S.ColumnMetadata(
        name="value",
        datatype="integer",
        required=True,
        privacy_id=False,
        nullable_proportion=0,
        partitions=["Red", "Blue"],
        max_num_partitions=2,
        max_length=5,
        max_groups_per_unit=2,
        max_contributions=2,
    )

    assert result == expected_result


def test_attach_partitions_partition_level(partitions_categorical):

    column_meta = S.ColumnMetadata(
        name="color", datatype="string", required=True, privacy_id=False, nullable_proportion=0
    )
    col_contrib_level = ContributionLevel.PARTITION
    result = attach_partitions_to_column(column_meta, partitions_categorical, col_contrib_level)
    expected_result = S.ColumnMetadata(
        name="color",
        datatype="string",
        required=True,
        privacy_id=False,
        nullable_proportion=0,
        partitions=partitions_categorical,
        max_num_partitions=2,
    )

    assert result == expected_result
