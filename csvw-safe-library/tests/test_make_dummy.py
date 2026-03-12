# test_make_dummy_from_metadata.py
import numpy as np
import pandas as pd
import pytest

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    DEPENDENCY_TYPE,
    DEPENDS_ON,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    TABLE_SCHEMA,
    VALUE_MAP,
    DependencyType,
)
from csvw_safe.datatypes import DataTypes
from csvw_safe.make_dummy_from_metadata import (
    _bigger_series,
    _fixed_series,
    _mapping_series,
    apply_nulls,
    generate_column,
    generate_column_series,
    generate_dependant_column_series,
    get_bounds,
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
    out = apply_nulls(s.copy(), 0.5, DataTypes.INTEGER, rng)
    assert out.isna().sum() >= 1
    assert len(out) == 10


def test_apply_nulls_datetime(rng):
    s = pd.Series(pd.date_range("2023-01-01", periods=5))
    out = apply_nulls(s.copy(), 0.4, DataTypes.DATETIME, rng)
    assert out.isna().sum() >= 1
    assert len(out) == 5


def test_get_bounds():
    col_meta = {MINIMUM: 0, MAXIMUM: 10, "name": "a"}
    assert get_bounds(col_meta) == (0, 10)


def test_generate_column_series_integer(rng):
    col_meta = {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 5, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= 0 and s.max() <= 5


def test_generate_column_series_double(rng):
    col_meta = {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0.0, MAXIMUM: 1.0, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= 0.0 and s.max() <= 1.0


def test_generate_column_series_boolean(rng):
    col_meta = {DATATYPE: DataTypes.BOOLEAN, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.dropna().isin([True, False]).all()


def test_generate_column_series_string(rng):
    col_meta = {DATATYPE: DataTypes.STRING, "max_num_partitions": 5, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert len(s) == 10


def test_bigger_series_numeric(rng):
    base = pd.Series(np.arange(10))
    col_meta = {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0, MAXIMUM: 20}
    s = _bigger_series(base, col_meta, 10, rng)
    assert (s > base).all()


def test_bigger_series_datetime(rng):
    base = pd.Series(pd.date_range("2023-01-01", periods=5))
    col_meta = {DATATYPE: DataTypes.DATETIME, MINIMUM: "2023-01-01", MAXIMUM: "2023-01-10"}
    s = _bigger_series(base, col_meta, 5, rng)
    assert (s > base).all()


def test_mapping_series(rng):
    base = pd.Series([1, 2, 1])
    col_meta = {DATATYPE: DataTypes.STRING, VALUE_MAP: {1: ["a", "b"], 2: ["c"]}}
    s = _mapping_series(base, col_meta, rng)
    assert all(val in ["a", "b", "c"] for val in s.dropna())


def test_fixed_series(rng):
    base = pd.Series([10, 10, 20, 20])
    col_meta = {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 20}
    s = _fixed_series(base, col_meta, rng)
    assert s.iloc[0] == s.iloc[1]
    assert s.iloc[2] == s.iloc[3]


def test_generate_dependant_column_series_bigger(rng):
    base = pd.Series(np.arange(5))
    col_meta = {
        DATATYPE: DataTypes.DOUBLE,
        MINIMUM: 0,
        MAXIMUM: 10,
        DEPENDENCY_TYPE: DependencyType.BIGGER,
    }
    s = generate_dependant_column_series(base, col_meta, 5, rng)
    assert (s > base).all()


def test_generate_dependant_column_series_mapping(rng):
    base = pd.Series([1, 2, 1])
    col_meta = {
        DATATYPE: DataTypes.STRING,
        DEPENDENCY_TYPE: DependencyType.MAPPING,
        VALUE_MAP: {1: ["x", "y"], 2: ["z"]},
    }
    s = generate_dependant_column_series(base, col_meta, 3, rng)
    assert all(val in ["x", "y", "z"] for val in s)


def test_generate_dependant_column_series_fixed(rng):
    base = pd.Series([10, 10, 20, 20])
    col_meta = {
        DATATYPE: DataTypes.DOUBLE,
        MINIMUM: 0,
        MAXIMUM: 5,
        DEPENDENCY_TYPE: DependencyType.FIXED,
    }
    s = generate_dependant_column_series(base, col_meta, 4, rng)
    assert s.iloc[0] == s.iloc[1]
    assert s.iloc[2] == s.iloc[3]


def test_generate_column_recursive(rng):
    columns_meta = [
        {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 5, COL_NAME: "a"},
        {
            DATATYPE: DataTypes.DOUBLE,
            MINIMUM: 0,
            MAXIMUM: 10,
            COL_NAME: "b",
            DEPENDS_ON: "a",
            DEPENDENCY_TYPE: DependencyType.BIGGER,
        },
    ]
    depends_map = {col[COL_NAME]: col.get(DEPENDS_ON) for col in columns_meta}
    data_dict = {}
    data_dict = generate_column("b", columns_meta, depends_map, data_dict, 5, rng)
    assert "b" in data_dict and "a" in data_dict
    assert (data_dict["b"] > data_dict["a"]).all()


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
    print("+++++++++++")
    print(df)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 5
