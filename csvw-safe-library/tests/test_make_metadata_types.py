import pytest
import pandas as pd
import numpy as np
from csvw_safe.make_metadata_from_data import (
    is_small_categorical_integer,
    is_small_datetime,
    infer_csvw_datatype,
    is_categorical,
)


@pytest.fixture
def numeric_series():
    return pd.Series([1, 2, 3, 4, 5])


@pytest.fixture
def float_series():
    return pd.Series([1.1, 2.2, 3.3])


@pytest.fixture
def small_int_series_with_nan():
    return pd.Series([1, 2, 2, np.nan, 1])


@pytest.fixture
def large_int_series():
    return pd.Series(range(50))


@pytest.fixture
def datetime_series():
    return pd.Series(pd.date_range("2025-01-01", periods=5))


@pytest.fixture
def datetime_series_large():
    return pd.Series(pd.date_range("2025-01-01", periods=30))


@pytest.fixture
def bool_series():
    return pd.Series([True, False, True])


@pytest.fixture
def string_series():
    return pd.Series(["a", "b", "c", "a"])


@pytest.fixture
def empty_series():
    return pd.Series([], dtype=float)


# ==========================
# Tests
# ==========================


def test_is_small_categorical_integer_basic(numeric_series):
    assert is_small_categorical_integer(numeric_series) is True


def test_is_small_categorical_integer_float(float_series):
    assert is_small_categorical_integer(float_series) == False


def test_is_small_categorical_integer_with_nan(small_int_series_with_nan):
    assert is_small_categorical_integer(small_int_series_with_nan) is True


def test_is_small_categorical_integer_large(large_int_series):
    assert is_small_categorical_integer(large_int_series) is False


def test_is_small_categorical_integer_empty(empty_series):
    assert is_small_categorical_integer(empty_series) is False


def test_is_small_datetime_basic(datetime_series):
    assert is_small_datetime(datetime_series) is True


def test_is_small_datetime_large(datetime_series_large):
    assert is_small_datetime(datetime_series_large) is False


def test_is_small_datetime_non_datetime(numeric_series):
    assert is_small_datetime(numeric_series) is False


def test_infer_csvw_datatype_boolean(bool_series):
    assert infer_csvw_datatype(bool_series) == "boolean"


def test_infer_csvw_datatype_datetime(datetime_series):
    assert infer_csvw_datatype(datetime_series) == "dateTime"


def test_infer_csvw_datatype_numeric(numeric_series):
    assert infer_csvw_datatype(numeric_series) == "double"


def test_infer_csvw_datatype_string(string_series):
    assert infer_csvw_datatype(string_series) == "string"


def test_is_categorical_boolean(bool_series):
    assert is_categorical(bool_series) is True


def test_is_categorical_small_int(small_int_series_with_nan):
    assert is_categorical(small_int_series_with_nan) is True


def test_is_categorical_large_int(large_int_series):
    # large numeric integer series is not small -> not categorical
    assert is_categorical(large_int_series) is False


def test_is_categorical_small_datetime(datetime_series):
    assert is_categorical(datetime_series) is True


def test_is_categorical_large_datetime(datetime_series_large):
    assert is_categorical(datetime_series_large) is False


def test_is_categorical_string(string_series):
    assert is_categorical(string_series) is True


def test_is_categorical_float(float_series):
    # float numeric series -> not categorical
    assert is_categorical(float_series) is False
