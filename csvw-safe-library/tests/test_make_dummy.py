import numpy as np
import pandas as pd
import pytest

from csvw_safe.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    DATATYPE,
    EXHAUSTIVE_PARTITIONS,
    LOWER_BOUND,
    MAXIMUM,
    MINIMUM,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_KEYS,
    PUBLIC_PARTITIONS,
    TABLE_SCHEMA,
    UPPER_BOUND,
)
from csvw_safe.datatypes import DataTypes
from csvw_safe.make_dummy_from_metadata import (
    _apply_value_mask,
    _predicate_mask,
    apply_nulls_serie,
    column_group_partitions,
    make_dummy_from_metadata,
)


@pytest.fixture
def rng():
    return np.random.default_rng(42)


@pytest.fixture
def sample_metadata():
    return [
        {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 10, "name": "a"},
        {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0.0, MAXIMUM: 1.0, "name": "b"},
        {DATATYPE: DataTypes.STRING, "name": "c", "max_num_partitions": 5},
    ]


def test_apply_nulls_numeric(rng):
    s = pd.Series(np.arange(10))
    out = apply_nulls_serie(s.copy(), 0.5, DataTypes.INTEGER, rng)
    assert out.isna().sum() >= 1
    assert len(out) == 10


def test_apply_nulls_datetime(rng):
    s = pd.Series(pd.date_range("2023-01-01", periods=5))
    out = apply_nulls_serie(s.copy(), 0.4, DataTypes.DATETIME, rng)
    assert out.isna().sum() >= 1
    assert len(out) == 5


def test_make_dummy_from_metadata_basic(rng):
    metadata = {
        TABLE_SCHEMA: {
            COL_LIST: [
                {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 5, COL_NAME: "col1"},
                {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0, MAXIMUM: 10, COL_NAME: "col2"},
            ]
        }
    }
    df = make_dummy_from_metadata(metadata, nb_rows=5, seed=42)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 5


def test_apply_value_mask_categorical():
    series = pd.Series(["a", "b", "c", "a", "b"])
    value = {PARTITION_VALUE: "a"}
    mask = _apply_value_mask(series, value)
    expected = pd.Series([True, False, False, True, False])
    pd.testing.assert_series_equal(mask, expected)


def test_apply_value_mask_continuous():
    series = pd.Series([1, 2, 3, 4, 5])
    value = {LOWER_BOUND: 2, UPPER_BOUND: 4}
    mask = _apply_value_mask(series, value)
    expected = pd.Series([False, True, True, True, False])
    pd.testing.assert_series_equal(mask, expected)


def test_apply_value_mask_scalar():
    series = pd.Series([1, 2, 3])
    value = 2
    mask = _apply_value_mask(series, value)
    expected = pd.Series([False, True, False])
    pd.testing.assert_series_equal(mask, expected)


def test_predicate_mask_single_column():
    df = pd.DataFrame({"col1": [1, 2, 3, 4]})
    predicate = {"col1": {LOWER_BOUND: 2, UPPER_BOUND: 3}}
    mask = _predicate_mask(df, predicate)
    expected = pd.Series([False, True, True, False])
    pd.testing.assert_series_equal(mask, expected)


def test_predicate_mask_multiple_columns():
    df = pd.DataFrame({"col1": ["a", "b", "a"], "col2": [10, 20, 30]})
    predicate = {"col1": {PARTITION_VALUE: "a"}, "col2": {LOWER_BOUND: 5, UPPER_BOUND: 25}}
    mask = _predicate_mask(df, predicate)
    expected = pd.Series([True, False, False])
    pd.testing.assert_series_equal(mask, expected)


def test_column_group_partitions_public_partitions():
    df = pd.DataFrame({"col1": ["a", "b", "a", "b"], "col2": [1, 2, 3, 4]})
    columns_group_meta = [
        {
            EXHAUSTIVE_PARTITIONS: True,
            PUBLIC_PARTITIONS: [
                {
                    PREDICATE: {
                        "col1": {PARTITION_VALUE: "a"},
                        "col2": {LOWER_BOUND: 3, UPPER_BOUND: 4},
                    }
                }
            ],
        }
    ]
    filtered = column_group_partitions(df, columns_group_meta)
    expected = pd.DataFrame({"col1": ["a"], "col2": [3]}).reset_index(drop=True)
    pd.testing.assert_frame_equal(filtered, expected)


def test_column_group_partitions_public_keys_fallback():
    df = pd.DataFrame({"col1": ["a", "b", "a", "b"], "col2": [1, 2, 3, 4]})
    columns_group_meta = [
        {
            EXHAUSTIVE_PARTITIONS: True,
            PUBLIC_KEYS: [
                {"col1": {PARTITION_VALUE: "a"}, "col2": {LOWER_BOUND: 1, UPPER_BOUND: 2}},
                {"col1": {PARTITION_VALUE: "b"}, "col2": {LOWER_BOUND: 3, UPPER_BOUND: 4}},
            ],
        }
    ]
    filtered = column_group_partitions(df, columns_group_meta)
    expected = pd.DataFrame({"col1": ["a", "b"], "col2": [1, 4]}).reset_index(drop=True)
    pd.testing.assert_frame_equal(filtered, expected)


def test_column_group_partitions_not_exhaustive():
    df = pd.DataFrame({"col1": ["a", "b", "a", "b"], "col2": [1, 2, 3, 4]})
    columns_group_meta = [
        {
            EXHAUSTIVE_PARTITIONS: False,
            PUBLIC_KEYS: [
                {"col1": {PARTITION_VALUE: "a"}, "col2": {LOWER_BOUND: 1, UPPER_BOUND: 2}},
                {"col1": {PARTITION_VALUE: "b"}, "col2": {LOWER_BOUND: 3, UPPER_BOUND: 4}},
            ],
        }
    ]
    filtered = column_group_partitions(df, columns_group_meta)
    pd.testing.assert_frame_equal(filtered, df)


def test_make_dummy_applies_column_group_partitions():
    metadata = {
        TABLE_SCHEMA: {
            COL_LIST: [
                {
                    COL_NAME: "col1",
                    DATATYPE: DataTypes.STRING,
                }
            ]
        },
        ADD_INFO: [
            {
                EXHAUSTIVE_PARTITIONS: True,
                PUBLIC_PARTITIONS: [
                    {
                        PREDICATE: {
                            "col1": {PARTITION_VALUE: "a"},
                        }
                    }
                ],
            }
        ],
    }

    df = make_dummy_from_metadata(metadata, nb_rows=20, seed=0)

    # Assert filtering happened: only "a" should remain
    assert (df["col1"] == "a").all()
