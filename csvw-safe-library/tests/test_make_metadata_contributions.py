import pytest
import pandas as pd
import numpy as np
from csvw_safe.make_metadata_from_data import (
    attach_partitions_to_column,
    build_partitions,
    column_level_continuous_partition,
    keep_predicate_only,
    make_predicate,
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
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": "blue"},
            "csvw-safe:bounds.maxLength": 2,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": "red"},
            "csvw-safe:bounds.maxLength": 3,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
    ]
    assert partitions == expected_partitions


def test_numeric_partition(simple_df):
    column_specs = [
        {
            "name": "value",
            "kind": "numeric",
            "bins": [0, 25, 50, 60],
            "is_datetime": False,
        }
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)
    # There should be three bins: [0-25), [25-50), [50-60)
    expected_partitions = [
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"lowerBound": 0.0, "upperBound": 25.0},
            "csvw-safe:bounds.maxLength": 2,
            "csvw-safe:bounds.maxGroupsPerUnit": 2,
            "csvw-safe:bounds.maxContributions": 1,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"lowerBound": 25.0, "upperBound": 50.0},
            "csvw-safe:bounds.maxLength": 2,
            "csvw-safe:bounds.maxGroupsPerUnit": 2,
            "csvw-safe:bounds.maxContributions": 1,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"lowerBound": 50.0, "upperBound": 60.0},
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 1,
        },
    ]
    assert partitions == expected_partitions


def test_mixed_partitions(simple_df):
    column_specs = [
        {"name": "color", "kind": "categorical", "is_datetime": False},
        {
            "name": "value",
            "kind": "numeric",
            "bins": [0, 25, 50, 60],
            "is_datetime": False,
        },
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)
    expected_partitions = [
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "color": {"partitionValue": "blue"},
                "value": {"lowerBound": 0.0, "upperBound": 25.0},
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "color": {"partitionValue": "blue"},
                "value": {"lowerBound": 25.0, "upperBound": 50.0},
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "color": {"partitionValue": "red"},
                "value": {"lowerBound": 0.0, "upperBound": 25.0},
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "color": {"partitionValue": "red"},
                "value": {"lowerBound": 25.0, "upperBound": 50.0},
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "color": {"partitionValue": "red"},
                "value": {"lowerBound": 50.0, "upperBound": 60.0},
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 1,
        },
    ]
    assert partitions == expected_partitions


def test_datetime_partition(simple_df):
    column_specs = [
        {
            "name": "timestamp",
            "kind": "numeric",
            "bins": pd.date_range("2025-01-01", periods=6),
            "is_datetime": True,
        }
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)
    expected_partitions = [
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": "2025-01-01T00:00:00",
                "upperBound": "2025-01-02T00:00:00",
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": "2025-01-02T00:00:00",
                "upperBound": "2025-01-03T00:00:00",
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": "2025-01-03T00:00:00",
                "upperBound": "2025-01-04T00:00:00",
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": "2025-01-04T00:00:00",
                "upperBound": "2025-01-05T00:00:00",
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": "2025-01-05T00:00:00",
                "upperBound": "2025-01-06T00:00:00",
            },
            "csvw-safe:bounds.maxLength": 1,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 1,
        },
    ]
    assert partitions == expected_partitions


@pytest.fixture
def partitions_mixed():
    return [
        {
            "csvw-safe:predicate": {"partitionValue": "Red"},
            "csvw-safe:bounds.maxLength": 3,
            "csvw-safe:bounds.maxGroupsPerUnit": 2,
            "csvw-safe:bounds.maxContributions": 1,
        },
        {
            "csvw-safe:predicate": {"partitionValue": "Blue"},
            "csvw-safe:bounds.maxLength": 5,
            "csvw-safe:bounds.maxGroupsPerUnit": 1,
            "csvw-safe:bounds.maxContributions": 2,
        },
        {
            "csvw-safe:predicate": {"lowerBound": 0.0, "upperBound": 10.0},
            "csvw-safe:bounds.maxLength": 4,
            "csvw-safe:bounds.maxGroupsPerUnit": 2,
            "csvw-safe:bounds.maxContributions": 1,
        },
        {
            "csvw-safe:predicate": {"lowerBound": 10.0, "upperBound": 20.0},
            "csvw-safe:bounds.maxLength": 6,
            "csvw-safe:bounds.maxGroupsPerUnit": 3,
            "csvw-safe:bounds.maxContributions": 2,
        },
    ]


def test_column_level_continuous_partition(partitions_mixed):
    col_max = column_level_continuous_partition(partitions_mixed)
    expected = {
        "csvw-safe:bounds.maxLength": 6,
        "csvw-safe:bounds.maxGroupsPerUnit": 3,
        "csvw-safe:bounds.maxContributions": 2,
    }
    assert col_max == expected


def test_keep_predicate_only(partitions_mixed):
    keys = keep_predicate_only(partitions_mixed)
    expected = [
        "Red",
        "Blue",
        {"lowerBound": 0.0, "upperBound": 10.0},
        {"lowerBound": 10.0, "upperBound": 20.0},
    ]
    assert keys == expected


def test_attach_partitions_to_column_column_level(partitions_mixed):
    column_meta = {"name": "value", "kind": "numeric"}
    col_contrib_level = "column"
    result = attach_partitions_to_column(
        column_meta.copy(), partitions_mixed, col_contrib_level
    )
    expected_result = {
        "name": "value",
        "kind": "numeric",
        "csvw-safe:bounds.maxLength": 6,
        "csvw-safe:bounds.maxGroupsPerUnit": 3,
        "csvw-safe:bounds.maxContributions": 2,
        "csvw-safe:public.partitions": [
            "Red",
            "Blue",
            {"lowerBound": 0.0, "upperBound": 10.0},
            {"lowerBound": 10.0, "upperBound": 20.0},
        ],
        "csvw-safe:public.maxNumPartitions": 4,
    }
    # Max number of partitions
    assert result == expected_result


def test_attach_partitions_to_column_keep_partitions_only(partitions_mixed):
    column_meta = {"name": "color", "kind": "categorical"}
    col_contrib_level = "partition"
    result = attach_partitions_to_column(
        column_meta.copy(), partitions_mixed, col_contrib_level
    )
    expected = {
        "name": "color",
        "kind": "categorical",
        "csvw-safe:public.partitions": [
            {
                "csvw-safe:predicate": {"partitionValue": "Red"},
                "csvw-safe:bounds.maxLength": 3,
                "csvw-safe:bounds.maxGroupsPerUnit": 2,
                "csvw-safe:bounds.maxContributions": 1,
            },
            {
                "csvw-safe:predicate": {"partitionValue": "Blue"},
                "csvw-safe:bounds.maxLength": 5,
                "csvw-safe:bounds.maxGroupsPerUnit": 1,
                "csvw-safe:bounds.maxContributions": 2,
            },
            {
                "csvw-safe:predicate": {"lowerBound": 0.0, "upperBound": 10.0},
                "csvw-safe:bounds.maxLength": 4,
                "csvw-safe:bounds.maxGroupsPerUnit": 2,
                "csvw-safe:bounds.maxContributions": 1,
            },
            {
                "csvw-safe:predicate": {"lowerBound": 10.0, "upperBound": 20.0},
                "csvw-safe:bounds.maxLength": 6,
                "csvw-safe:bounds.maxGroupsPerUnit": 3,
                "csvw-safe:bounds.maxContributions": 2,
            },
        ],
        "csvw-safe:public.maxNumPartitions": 4,
    }
    assert result == expected
