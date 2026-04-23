import numpy as np
import pandas as pd
import pytest

from csvw_safe.constants import (
    COL_NAME,
    DATATYPE,
    DEFAULT_NUMBER_PARTITIONS,
    DEPENDENCY_TYPE,
    DEPENDS_ON,
    EXHAUSTIVE_KEYS,
    KEY_VALUES,
    MAX_NUM_PARTITIONS,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
    RANDOM_STRINGS,
    ROW_DEP,
    VALUE_MAP,
    DependencyType,
)
from csvw_safe.datatypes import DataTypes
from csvw_safe.generate_series import (
    bigger_series,
    fixed_series,
    mapping_series,
    generate_column_series,
    generate_dataframe,
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
    col_meta = {DATATYPE: DataTypes.INTEGER, MINIMUM: -10, MAXIMUM: 10, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= -10 and s.max() <= 10


def test_generate_column_series_positive_integer(rng):
    col_meta = {
        DATATYPE: DataTypes.POSITIVE_INTEGER,
        MINIMUM: 0,
        MAXIMUM: 5,
        NULL_PROP: 0,
    }
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= 0 and s.max() <= 5


def test_generate_column_series_negative_integer(rng):
    col_meta = {
        DATATYPE: DataTypes.NEGATIVE_INTEGER,
        MINIMUM: -10,
        MAXIMUM: -5,
        NULL_PROP: 0,
    }
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= -10 and s.max() <= -5


def test_generate_column_series_double(rng):
    col_meta = {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0.0, MAXIMUM: 1.0, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.min() >= 0.0 and s.max() <= 1.0


def test_generate_column_series_boolean(rng):
    col_meta = {DATATYPE: DataTypes.BOOLEAN, NULL_PROP: 0}
    s = generate_column_series(col_meta, 10, rng)
    assert s.dropna().isin([True, False]).all()


@pytest.mark.parametrize(
    "col_meta,nb_rows,expected_values",
    [
        # KEY_VALUES as strings
        ({DATATYPE: DataTypes.STRING, KEY_VALUES: ["x", "y"]}, 10, {"x", "y"}),
        # KEY_VALUES as dicts
        (
            {
                DATATYPE: DataTypes.STRING,
                KEY_VALUES: [{PARTITION_VALUE: "p1"}, {PARTITION_VALUE: "p2"}],
            },
            5,
            {"p1", "p2"},
        ),
        # KEY_VALUES as strings + dict with PARTITION_VALUE
        (
            {
                DATATYPE: DataTypes.STRING,
                KEY_VALUES: ["k1", {PARTITION_VALUE: "k2"}],
            },
            5,
            {"k1", "k2"},
        ),
        # PUBLIC_PARTITIONS as strings
        (
            {DATATYPE: DataTypes.STRING, PUBLIC_PARTITIONS: ["pp1", "pp2"]},
            8,
            {"pp1", "pp2"},
        ),
        # PUBLIC_PARTITIONS as dicts with PREDICATE
        (
            {
                DATATYPE: DataTypes.STRING,
                PUBLIC_PARTITIONS: [
                    {PREDICATE: {PARTITION_VALUE: "pp3"}},
                    {PREDICATE: {PARTITION_VALUE: "pp4"}},
                ],
            },
            6,
            {"pp3", "pp4"},
        ),
        # EXHAUSTIVE_KEYS=False triggers extra randoms
        (
            {
                DATATYPE: DataTypes.STRING,
                PUBLIC_PARTITIONS: ["p1"],
                EXHAUSTIVE_KEYS: False,
                MAX_NUM_PARTITIONS: 3,
            },
            6,
            {"p1", "a", "b"},
        ),
        # MAX_NUM_PARTITIONS branch
        ({DATATYPE: DataTypes.STRING, MAX_NUM_PARTITIONS: 4}, 5, {"a", "b", "c", "d"}),
        # Default fallback
        (
            {DATATYPE: DataTypes.STRING},
            3,
            set(RANDOM_STRINGS[:DEFAULT_NUMBER_PARTITIONS]),
        ),
    ],
)
def test_generate_string_column_branches(col_meta, nb_rows, expected_values):
    rng = np.random.default_rng(42)
    s = generate_column_series(col_meta, nb_rows, rng)
    # Check length
    assert len(s) == nb_rows
    # Check all values are strings
    assert all(isinstance(v, str) for v in s)
    # Check all values are in expected set
    assert set(s).issubset(expected_values)


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


def test_generate_column_series_duration(rng):
    """Test generating a duration column using generate_column_series."""
    col_meta = {
        DATATYPE: DataTypes.DURATION,
        MINIMUM: 0,  # seconds
        MAXIMUM: 3600,  # 1 hour in seconds
        NULL_PROP: 0,
    }

    s = generate_column_series(col_meta, 10, rng)

    # Length
    assert len(s) == 10

    # Check dtype
    assert pd.api.types.is_timedelta64_dtype(s)

    # Bounds
    lower = pd.to_timedelta(col_meta[MINIMUM], unit="s")
    upper = pd.to_timedelta(col_meta[MAXIMUM], unit="s")

    assert (s >= lower).all() and (s <= upper).all()


def test_bigger_series_numeric(rng):
    base = pd.Series(np.arange(10))
    col_meta = {DATATYPE: DataTypes.DOUBLE, MINIMUM: 0, MAXIMUM: 20}
    s = bigger_series(base, col_meta, 10, rng)
    assert (s > base).all()


def test_bigger_series_datetime(rng):
    base = pd.Series(pd.date_range("2023-01-01", periods=5))
    col_meta = {
        DATATYPE: DataTypes.DATETIME,
        MINIMUM: "2023-01-01",
        MAXIMUM: "2023-01-10",
    }
    s = bigger_series(base, col_meta, 5, rng)
    assert (s > base).all()


def test_bigger_series_duration(rng):
    base = pd.Series(pd.to_timedelta([1, 2, 3], unit="s"))
    col_meta = {
        DATATYPE: DataTypes.DURATION,
        MINIMUM: "0s",
        MAXIMUM: "10s",
    }
    s = bigger_series(base, col_meta, 3, rng)
    # Each value must be bigger than the base
    assert (s > base).all()
    # Values must stay within bounds
    assert (s <= pd.to_timedelta("10s")).all()
    assert (s >= pd.to_timedelta("0s")).all()


def test_mapping_series(rng):
    base = pd.Series([1, 2, 1])
    col_meta = {DATATYPE: DataTypes.STRING, VALUE_MAP: {1: ["a", "b"], 2: ["c"]}}
    s = mapping_series(base, {1: ["a", "b"], 2: ["c"]}, col_meta, rng)
    assert all(val in ["a", "b", "c"] for val in s.dropna())


def test_fixed_series(rng):
    base = pd.Series([10, 10, 20, 20])
    col_meta = {DATATYPE: DataTypes.INTEGER, MINIMUM: 0, MAXIMUM: 20}
    s = fixed_series(base, col_meta, rng)
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
    s = bigger_series(base, col_meta, 5, rng)
    assert (s > base).all()


def test_generate_dependant_column_series_mapping(rng):
    base = pd.Series([1, 2, 1])
    value_dict = {1: ["x", "y"], 2: ["z"]}
    col_meta = {
        DATATYPE: DataTypes.STRING,
        DEPENDENCY_TYPE: DependencyType.MAPPING,
        VALUE_MAP: value_dict,
    }
    s = mapping_series(base, value_dict, col_meta, rng)
    assert all(val in ["x", "y", "z"] for val in s)


def test_generate_dependant_column_series_fixed(rng):
    base = pd.Series([10, 10, 20, 20])
    col_meta = {
        DATATYPE: DataTypes.DOUBLE,
        MINIMUM: 0,
        MAXIMUM: 5,
        DEPENDENCY_TYPE: DependencyType.FIXED,
    }
    s = fixed_series(base, col_meta, rng)
    assert s.iloc[0] == s.iloc[1]
    assert s.iloc[2] == s.iloc[3]


def test_generate_dataframe_bigger_dependency(rng):
    nb_rows = 5

    columns_meta = [
        {
            COL_NAME: "a",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 0,
            MAXIMUM: 5,
        },
        {
            COL_NAME: "b",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 0,
            MAXIMUM: 10,
            ROW_DEP: [
                {
                    DEPENDS_ON: "a",
                    DEPENDENCY_TYPE: DependencyType.BIGGER,
                }
            ],
        },
    ]

    depends_map = {c[COL_NAME]: c.get(ROW_DEP, []) for c in columns_meta}

    meta_map = {c[COL_NAME]: c for c in columns_meta}
    order = ["a", "b"]

    df = generate_dataframe(depends_map, order, meta_map, nb_rows, rng)

    assert "a" in df and "b" in df
    assert (df["b"] >= df["a"]).all()


def test_generate_dataframe_circular_dependency(rng):
    nb_rows = 5

    columns_meta = [
        {
            COL_NAME: "col1",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 0,
            MAXIMUM: 10,
            ROW_DEP: [
                {
                    DEPENDS_ON: "col2",
                    DEPENDENCY_TYPE: DependencyType.BIGGER,
                }
            ],
        },
        {
            COL_NAME: "col2",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 5,
            MAXIMUM: 15,
            ROW_DEP: [
                {
                    DEPENDS_ON: "col1",
                    DEPENDENCY_TYPE: DependencyType.BIGGER,
                }
            ],
        },
    ]

    depends_map = {c[COL_NAME]: c.get(ROW_DEP, []) for c in columns_meta}

    meta_map = {c[COL_NAME]: c for c in columns_meta}
    order = ["col1", "col2"]  # arbitrary but deterministic

    df = generate_dataframe(depends_map, order, meta_map, nb_rows, rng)

    assert "col1" in df and "col2" in df
    assert len(df) == nb_rows
    assert isinstance(df["col1"], pd.Series)
    assert isinstance(df["col2"], pd.Series)


def test_generate_dataframe_already_generated_column(rng):
    nb_rows = 3

    columns_meta = [
        {
            COL_NAME: "x",
            DATATYPE: DataTypes.INTEGER,
            MINIMUM: 0,
            MAXIMUM: 5,
        }
    ]

    depends_map = {c[COL_NAME]: c.get(ROW_DEP, []) for c in columns_meta}

    meta_map = {c[COL_NAME]: c for c in columns_meta}
    order = ["x"]

    df = generate_dataframe(depends_map, order, meta_map, nb_rows, rng)

    assert "x" in df
    assert len(df["x"]) == nb_rows
