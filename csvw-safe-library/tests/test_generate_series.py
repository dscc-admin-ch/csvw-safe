import numpy as np
import pandas as pd
import pytest

from csvw_safe.constants import (
    COL_NAME,
    DATATYPE,
    DEPENDENCY_TYPE,
    DEPENDS_ON,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    VALUE_MAP,
    DependencyType,
)
from csvw_safe.datatypes import DataTypes
from csvw_safe.generate_series import (
    _bigger_series,
    _fixed_series,
    _mapping_series,
    generate_column_series,
    generate_dependant_column_series,
    generate_series,
    get_bounds,
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


def test_generate_column_series_datetime(rng):
    """Test generating a datetime column using generate_column_series."""
    col_meta = {
        DATATYPE: DataTypes.DATETIME,
        MINIMUM: "2026-01-01",
        MAXIMUM: "2026-01-31",
        NULL_PROP: 0,
    }
    s = generate_column_series(col_meta, 10, rng)
    assert len(s) == 10
    # Check type
    assert pd.api.types.is_datetime64_any_dtype(s)
    # Check all values within bounds
    lower = pd.to_datetime(col_meta[MINIMUM])
    upper = pd.to_datetime(col_meta[MAXIMUM])
    assert (s >= lower).all() and (s <= upper).all()


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


def test_generate_series_recursive(rng):
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
    data_dict = generate_series("b", columns_meta, depends_map, data_dict, 5, rng)
    assert "b" in data_dict and "a" in data_dict
    assert (data_dict["b"] > data_dict["a"]).all()


def test_generate_column_circular_dependency(rng):
    nb_rows = 5

    # Columns with circular dependency
    columns_meta = [
        {
            COL_NAME: "col1",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 0,
            MAXIMUM: 10,
            NULL_PROP: 0,
            DEPENDENCY_TYPE: DependencyType.BIGGER,
        },
        {
            COL_NAME: "col2",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 5,
            MAXIMUM: 15,
            NULL_PROP: 0,
            DEPENDENCY_TYPE: DependencyType.BIGGER,
        },
    ]

    depends_map = {
        "col1": "col2",
        "col2": "col1",
    }

    data_dict = {}

    # Force col1 to see col2 as visited → triggers circular dependency branch
    visited = {"col2"}

    data_dict = generate_series(
        "col1",
        columns_meta,
        depends_map,
        data_dict,
        nb_rows,
        rng,
        visited=visited,
        max_recursion=10,
        depth=0,
    )

    # The circular branch should generate col1 using generate_column_series
    assert "col1" in data_dict
    assert isinstance(data_dict["col1"], pd.Series)
    assert len(data_dict["col1"]) == nb_rows
