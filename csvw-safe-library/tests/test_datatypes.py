import numpy as np
import pandas as pd
import pytest

from csvw_safe.datatypes import (
    DataTypes,
    infer_xmlschema_datatype,
    is_categorical,
    is_date,
    is_datetime,
    refine_integer_type,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2023-03-25", True),
        ("2023-03-25T12:34:56", False),  # datetime, not date
        ("25-03-2023", False),  # wrong format
        ("2023-3-5", False),  # wrong format (< 10)
        ("not a date", False),
        (None, False),
        (12345, False),
    ],
)
def test_is_date(value, expected):
    assert is_date(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2023-03-25T12:34:56", True),
        ("2023-03-25", True),  # date also valid ISO format
        ("2023-03-25 12:34:56", True),
        ("2023-03-05T01:02:03", True),
        ("2023-3-5T01:02:03", False),
        ("invalid", False),
        (None, False),
        (123456, False),
    ],
)
def test_is_datetime(value, expected):
    assert is_datetime(value) == expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([1, 2, 3, 4]), True),
        (pd.Series([1, 2, 2, np.nan, 1]), True),
        (pd.Series(range(20)), True),  # boundary
        (pd.Series(range(21)), False),  # boundary overflow
        (pd.Series(range(50)), False),
    ],
)
def test_is_categorical_integer(series, expected):
    assert is_categorical(series) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series(pd.date_range("2025-01-01", periods=5)), True),
        (pd.Series(pd.date_range("2025-01-01", periods=20)), True),
        (pd.Series(pd.date_range("2025-01-01", periods=21)), False),
        (pd.Series(pd.date_range("2024-01-01", periods=3).tolist() + [pd.NaT]), True),
        (pd.Series([], dtype="datetime64[ns]"), True),
    ],
)
def test_is_categorical_datetime(series, expected):
    assert is_categorical(series) is expected


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
        (pd.Series([1.1, 2.2, 3.3]), True),
        (pd.Series([np.nan, np.nan]), True),
    ],
)
def test_is_categorical_all(series, expected):
    assert is_categorical(series) is expected


def test_refine_integer_type():
    # positive integer
    s = pd.Series([1, 2, 3])
    assert refine_integer_type(s) == DataTypes.POSITIVE_INTEGER

    # non-negative integer including zero
    s = pd.Series([0, 1, 2])
    assert refine_integer_type(s) == DataTypes.INTEGER

    # negative integer
    s = pd.Series([-5, -2, -1])
    assert refine_integer_type(s) == DataTypes.NEGATIVE_INTEGER

    # non-positive integer including zero
    s = pd.Series([-3, 0, -1])
    assert refine_integer_type(s) == DataTypes.INTEGER

    # mixed integers
    s = pd.Series([-1, 0, 1])
    assert refine_integer_type(s) == DataTypes.INTEGER


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series([True, False]), DataTypes.BOOLEAN),
        (pd.Series([True, False, pd.NA]), DataTypes.BOOLEAN),
        (pd.Series([-1, 2, 3]), DataTypes.INTEGER),
        (pd.Series([-1, 2, np.nan, 3]), DataTypes.INTEGER),
        (pd.Series([-1, 2, pd.NA, 3]), DataTypes.INTEGER),
        (pd.Series([0, 2, 3]), DataTypes.INTEGER),
        (pd.Series([1, 2, 3]), DataTypes.POSITIVE_INTEGER),
        (pd.Series([1.0, 2.0, 3.0]), DataTypes.POSITIVE_INTEGER),
        (pd.Series([1.2, 3.4]), DataTypes.DOUBLE),
        (pd.Series([1, 2.5, 3]), DataTypes.DOUBLE),
        (pd.Series([1.5, 2, 3]), DataTypes.DOUBLE),
        (pd.Series([1.1, 2.2, pd.NA]), DataTypes.DOUBLE),
        (pd.Series(pd.date_range("2025-01-01", periods=3)), DataTypes.DATETIME),
        (pd.Series(["2023-03-25T12:00:00", "2023-03-26T01:23:45"]), DataTypes.DATETIME),
        (pd.Series([pd.Timestamp("2025-01-01"), pd.NA]), DataTypes.DATETIME),
        (pd.Series(["2023-03-25", "2023-01-01"]), DataTypes.DATE),
        (pd.Series(pd.to_timedelta([1, 2, 3], unit="s")), DataTypes.DURATION),
        (pd.Series([pd.Timedelta(seconds=1), pd.Timedelta(seconds=2)]), DataTypes.DURATION),
        (pd.Series([pd.Timedelta(seconds=1), pd.NA, pd.Timedelta(seconds=3)]), DataTypes.DURATION),
        (pd.Series(pd.to_timedelta(["1s", "2s", "3s"])), DataTypes.DURATION),
        (pd.Series([pd.NA, pd.NA]), DataTypes.STRING),
        (pd.Series(["a", "b"]), DataTypes.STRING),
        (pd.Series(["a", "b", pd.NA]), DataTypes.STRING),
        (pd.Series(["2023-01-01", "not-a-date"]), DataTypes.STRING),
        (pd.Series(["1s", "not-a-duration"]), DataTypes.STRING),
        (pd.Series(["", " "]), DataTypes.STRING),
        (pd.Series(["2023-01-01", 123]), DataTypes.STRING),
        # Design choice (to ensure dummy same format as original)
        (pd.Series(["1", "2", "3"]), DataTypes.STRING),
        (pd.Series(["1.5", "2", "3"]), DataTypes.STRING),
        (pd.Series(["True", "False"]), DataTypes.STRING),
        (pd.Series(["true", "false"]), DataTypes.STRING),
    ],
)
def test_infer_xmlschema_datatype(series, expected):
    assert infer_xmlschema_datatype(series) == expected
