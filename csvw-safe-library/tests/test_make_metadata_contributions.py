import pandas as pd
import pytest

from csvw_safe import constants as c
from csvw_safe.make_metadata_from_data import (
    build_partitions,
    make_metadata_from_data,
    make_numeric_partitions,
)
from csvw_safe.metadata_structure import (
    CategoricalPredicate,
    ContinuousPredicate,
    MultiColumnPartition,
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
            predicate=CategoricalPredicate(partition_value="blue"),
            max_length=2,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        SingleColumnPartition(
            predicate=CategoricalPredicate(partition_value="red"),
            max_length=3,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_numeric_partition(simple_df):
    column_specs = [
        {
            "name": "value",
            "kind": "continuous",
            "bins": [0, 25, 50, 60],
            "is_datetime": False,
        }
    ]
    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        SingleColumnPartition(
            predicate=ContinuousPredicate(lower_bound=0.0, upper_bound=25.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        SingleColumnPartition(
            predicate=ContinuousPredicate(lower_bound=25.0, upper_bound=50.0),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        SingleColumnPartition(
            predicate=ContinuousPredicate(lower_bound=50.0, upper_bound=60.0),
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=1,
        ),
    ]

    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]


def test_mixed_partitions(simple_df):
    column_specs = [
        {"name": "color", "kind": "categorical", "is_datetime": False},
        {
            "name": "value",
            "kind": "continuous",
            "bins": [0, 25, 50, 60],
            "is_datetime": False,
        },
    ]

    partitions = build_partitions(simple_df, "user_id", column_specs)

    expected_partitions = [
        MultiColumnPartition(
            predicate={
                "color": CategoricalPredicate(partition_value="blue"),
                "value": ContinuousPredicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": CategoricalPredicate(partition_value="blue"),
                "value": ContinuousPredicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": CategoricalPredicate(partition_value="red"),
                "value": ContinuousPredicate(lower_bound=0.0, upper_bound=25.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": CategoricalPredicate(partition_value="red"),
                "value": ContinuousPredicate(lower_bound=25.0, upper_bound=50.0),
            },
            max_length=1,
            max_groups_per_unit=1,
            max_contributions=2,
        ),
        MultiColumnPartition(
            predicate={
                "color": CategoricalPredicate(partition_value="red"),
                "value": ContinuousPredicate(lower_bound=50.0, upper_bound=60.0),
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
    color_column = next(col for col in metadata[c.TABLE_SCHEMA][c.COL_LIST] if col[c.COL_NAME] == "color")

    assert color_column[c.KEY_VALUES] == ["blue", "red"]
    assert color_column[c.MAX_NUM_PARTITIONS] == 2
    assert color_column[c.MAX_LENGTH] == 3
    assert color_column[c.MAX_GROUPS] == 1
    assert color_column[c.MAX_CONTRIB] == 2


def test_partition_level_partitions(simple_df):

    metadata = make_metadata_from_data(
        df=simple_df,
        privacy_unit="user_id",
        fine_contributions_level={"color": "partition"},
    )

    color_column = next(col for col in metadata[c.TABLE_SCHEMA][c.COL_LIST] if col[c.COL_NAME] == "color")

    assert isinstance(color_column[c.PUBLIC_PARTITIONS], list)
    assert isinstance(color_column[c.PUBLIC_PARTITIONS][0], dict)

    assert color_column[c.MAX_NUM_PARTITIONS] == 2


def test_datetime_partition(simple_df):
    # Create partitions for the datetime column
    bounds = ["2025-01-01", "2025-01-03", "2025-01-05"]
    partitions = make_numeric_partitions(simple_df, "user_id", "timestamp", bounds=bounds)

    # Expected predicates use ISO strings
    expected_partitions = [
        SingleColumnPartition(
            predicate=ContinuousPredicate(
                lower_bound=pd.Timestamp("2025-01-01T00:00:00").isoformat(),
                upper_bound=pd.Timestamp("2025-01-03T00:00:00").isoformat(),
            ),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
        SingleColumnPartition(
            predicate=ContinuousPredicate(
                lower_bound=pd.Timestamp("2025-01-03T00:00:00").isoformat(),
                upper_bound=pd.Timestamp("2025-01-05T00:00:00").isoformat(),
            ),
            max_length=2,
            max_groups_per_unit=2,
            max_contributions=1,
        ),
    ]

    # Compare the generated partitions to the expected partitions
    assert [p.to_dict() for p in partitions] == [p.to_dict() for p in expected_partitions]
