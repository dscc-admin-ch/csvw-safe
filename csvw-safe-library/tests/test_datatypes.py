import pytest
import pandas as pd
import numpy as np

from csvw_safe.datatypes import (
    is_small_categorical_integer,
    is_small_datetime,
    infer_xmlschema_datatype,
    is_categorical,
)


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([1, 2, 3, 4]), True),
        (pd.Series([1, 2, 2, np.nan, 1]), True),
        (pd.Series(range(20)), True),  # boundary
        (pd.Series(range(21)), False),  # boundary overflow
        (pd.Series(range(50)), False),
        (pd.Series([1.1, 2.2, 3.3]), False),
        (pd.Series(["a", "b"]), False),
        (pd.Series([], dtype=float), False),
    ],
)
def test_is_small_categorical_integer(series, expected):
    assert is_small_categorical_integer(series) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series(pd.date_range("2025-01-01", periods=5)), True),
        (pd.Series(pd.date_range("2025-01-01", periods=20)), True),
        (pd.Series(pd.date_range("2025-01-01", periods=21)), False),
        (pd.Series(pd.date_range("2024-01-01", periods=3).tolist() + [pd.NaT]), True),
        (pd.Series([], dtype="datetime64[ns]"), True),
        (pd.Series([1, 2, 3]), False),
        (pd.Series(["a", "b"]), False),
        (pd.Series([True, False]), False),
    ],
)
def test_is_small_datetime(series, expected):
    assert is_small_datetime(series) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([True, False]), "boolean"),
        (pd.Series([1, 2, 3]), "integer"),
        (pd.Series([1.2, 3.4]), "double"),
        (pd.Series(["a", "b"]), "string"),
        (pd.Series([1, 2.5, 3]), "double"),
        (pd.Series([1.5, 2, 3]), "double"),
        (pd.Series([1, 2, np.nan, 3]), "integer"),  # despite pandas
        (pd.Series(pd.date_range("2025-01-01", periods=3)), "dateTime"),
    ],
)
def test_infer_xmlschema_datatype(series, expected):
    assert infer_xmlschema_datatype(series) == expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([True, False]), True),
        (pd.Series([True, False, np.nan]), True),
        (pd.Series([1, 2, 2, np.nan, 1]), True),
        (pd.Series(range(50)), False),
        (pd.Series(pd.date_range("2025-01-01", periods=5)), True),
        (pd.Series(pd.date_range("2025-01-01", periods=30)), False),
        (pd.Series(["a", "b", "c"]), True),
        (pd.Series(["1", "2", "3"]), True),
        (pd.Series([1.1, 2.2, 3.3]), False),
        (pd.Series([np.nan, np.nan]), True),
    ],
)
def test_is_categorical(series, expected):
    assert is_categorical(series) is expected


# ============================================================
# Cross-function consistency
# ============================================================
def test_is_categorical_consistency():
    s = pd.Series(["a", "b", "a", "c"])

    assert is_categorical(s)
    assert not is_small_categorical_integer(s)


def test_small_integer_is_also_categorical():
    s = pd.Series([1, 2, 3, 1, 2])

    assert is_small_categorical_integer(s)
    assert is_categorical(s)


def test_large_integer_not_categorical():
    s = pd.Series(range(100))

    assert not is_small_categorical_integer(s)
    assert not is_categorical(s)
