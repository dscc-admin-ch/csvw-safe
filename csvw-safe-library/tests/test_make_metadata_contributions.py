import pytest
import pandas as pd
from csvw_safe import constants as C
from csvw_safe import metadata_structure as S
from csvw_safe.make_metadata_from_data import (
    attach_partitions_to_column,
    build_partitions,
    ContributionLevel,
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
        {
            '@type': C.PARTITION,
            C.PREDICATE: {C.PARTITION_VALUE: "blue"},
            C.MAX_LENGTH: 2,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {C.PARTITION_VALUE: "red"},
            C.MAX_LENGTH: 3,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
    ]
    # compare using dict representations
    assert [p.to_dict() for p in partitions] == expected_partitions


def test_numeric_partition(simple_df):
    column_specs = [{"name": "value", "kind": "numeric", "bins": [0, 25, 50, 60], "is_datetime": False}]
    partitions = build_partitions(simple_df, "user_id", column_specs)
    expected_partitions = [
        {
            '@type': C.PARTITION,
            C.PREDICATE: {C.LOWER_BOUND: 0.0, C.UPPER_BOUND: 25.0},
            C.MAX_LENGTH: 2,
            C.MAX_GROUPS: 2,
            C.MAX_CONTRIB: 1,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {C.LOWER_BOUND: 25.0, C.UPPER_BOUND: 50.0},
            C.MAX_LENGTH: 2,
            C.MAX_GROUPS: 2,
            C.MAX_CONTRIB: 1,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {C.LOWER_BOUND: 50.0, C.UPPER_BOUND: 60.0},
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 1,
        },
    ]
    assert [p.to_dict() for p in partitions] == expected_partitions


def test_mixed_partitions(simple_df):
    column_specs = [
        {"name": "color", "kind": "categorical", "is_datetime": False},
        {"name": "value", "kind": "numeric", "bins": [0, 25, 50, 60], "is_datetime": False},
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)
    expected_partitions = [
        {
            '@type': C.PARTITION,
            C.PREDICATE: {
                "color": {C.PARTITION_VALUE: "blue"},
                "value": {C.LOWER_BOUND: 0.0, C.UPPER_BOUND: 25.0},
            },
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {
                "color": {C.PARTITION_VALUE: "blue"},
                "value": {C.LOWER_BOUND: 25.0, C.UPPER_BOUND: 50.0},
            },
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {
                "color": {C.PARTITION_VALUE: "red"},
                "value": {C.LOWER_BOUND: 0.0, C.UPPER_BOUND: 25.0},
            },
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {
                "color": {C.PARTITION_VALUE: "red"},
                "value": {C.LOWER_BOUND: 25.0, C.UPPER_BOUND: 50.0},
            },
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 2,
        },
        {
            '@type': C.PARTITION,
            C.PREDICATE: {
                "color": {C.PARTITION_VALUE: "red"},
                "value": {C.LOWER_BOUND: 50.0, C.UPPER_BOUND: 60.0},
            },
            C.MAX_LENGTH: 1,
            C.MAX_GROUPS: 1,
            C.MAX_CONTRIB: 1,
        },
    ]
    assert [p.to_dict() for p in partitions] == expected_partitions


@pytest.fixture
def partitions_categorical():
    return [
        S.Partition(
            predicate=p[C.PREDICATE],
            max_length=p[C.MAX_LENGTH],
            max_groups_per_unit=p[C.MAX_GROUPS],
            max_contributions=p[C.MAX_CONTRIB],
        )
        for p in [
            {
                C.PREDICATE: {C.PARTITION_VALUE: "Red"},
                C.MAX_LENGTH: 3,
                C.MAX_GROUPS: 2,
                C.MAX_CONTRIB: 1,
            },
            {
                C.PREDICATE: {C.PARTITION_VALUE: "Blue"},
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
    result = attach_partitions_to_column(column_meta, partitions_categorical, col_contrib_level).to_dict()

    expected_result = {
        "@type": "csvw:Column",
        "name": "value",
        "datatype": "integer",
        "required": True,
        C.PRIVACY_ID: False,
        C.NULL_PROP: 0,
        C.PUBLIC_PARTITIONS: ["Red", "Blue"],
        C.MAX_NUM_PARTITIONS: 2,
        C.MAX_LENGTH: 5,
        C.MAX_GROUPS: 2,
        C.MAX_CONTRIB: 2,
    }

    assert result == expected_result


def test_attach_partitions_partition_level(partitions_categorical):

    column_meta = S.ColumnMetadata(
        name="color", datatype="string", required=True, privacy_id=False, nullable_proportion=0
    )
    col_contrib_level = ContributionLevel.PARTITION
    result = attach_partitions_to_column(column_meta, partitions_categorical, col_contrib_level).to_dict()
    expected_result = {
        "@type": "csvw:Column",
        "name": "color",
        "datatype": "string",
        "required": True,
        C.PRIVACY_ID: False,
        C.NULL_PROP: 0,
        C.PUBLIC_PARTITIONS: [p.to_dict() for p in partitions_categorical],
        C.MAX_NUM_PARTITIONS: 2,
    }

    assert result == expected_result
