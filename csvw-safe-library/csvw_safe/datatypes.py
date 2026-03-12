"""CSVW-SAFE Datatypes Utilities."""

from datetime import datetime
from enum import StrEnum
from typing import TypeVar

import pandas as pd


class DataTypes(StrEnum):
    """Column types for metadata."""

    STRING = "string"  # categorical
    BOOLEAN = "boolean"  # categorical
    INTEGER = "integer"  # categorical or continuous
    DOUBLE = "double"  # categorical or continuous
    DATETIME = "dateTime"  # categorical or continuous


class ColumnKind(StrEnum):
    """Partition Kind."""

    CATEGORICAL = "categorical"
    CONTINUOUS = "continuous"


T = TypeVar("T", int, float, datetime)


def is_small_categorical_integer(series: pd.Series, max_unique: int = 20) -> bool:
    """
    Determine if a numeric integer column with low cardinality should be treated as categorical.

    Parameters
    ----------
    series : pd.Series
        Input column.
    max_unique : int, default=20
        Maximum number of unique values to consider as categorical.

    Returns
    -------
    bool
        True if the column is integer and has low cardinality.
    """
    if not pd.api.types.is_numeric_dtype(series):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    is_integer = (non_null % 1 == 0).all()
    return bool(is_integer and non_null.nunique() <= max_unique)


def is_small_datetime(series: pd.Series, max_unique: int = 20) -> bool:
    """
    Detect whether a datetime column has low cardinality.

    Parameters
    ----------
    series : pd.Series
        Datetime column.
    max_unique : int, default=20
        Maximum number of unique values to treat as categorical-like.

    Returns
    -------
    bool
        True if datetime column has few unique values.
    """
    if not pd.api.types.is_datetime64_any_dtype(series):
        return False

    return bool(series.dropna().nunique() <= max_unique)


def infer_xmlschema_datatype(  # pylint: disable=too-many-return-statements
    series: pd.Series,
) -> DataTypes:
    """
    Infer an XML Schema (XSD 1.1) datatype for a pandas series.

    Parameters
    ----------
    series : pd.Series
        Input column.

    max_unique : int, default=20
        Maximum number of unique values to treat as categorical-like.

    Returns
    -------
    DataTypes
        Inferred XML Schema datatype.
    """
    s = series.dropna()

    if s.empty:
        return DataTypes.STRING

    if pd.api.types.is_bool_dtype(s):
        return DataTypes.BOOLEAN

    if pd.api.types.is_datetime64_any_dtype(s):
        return DataTypes.DATETIME

    if pd.api.types.is_integer_dtype(s):
        return DataTypes.INTEGER

    if pd.api.types.is_float_dtype(s):
        # pandas floats may contain integers due to NaN
        if (s % 1 == 0).all():
            return DataTypes.INTEGER
        return DataTypes.DOUBLE

    if pd.api.types.is_numeric_dtype(s):
        return DataTypes.INTEGER

    return DataTypes.STRING


def is_categorical(series: pd.Series, max_unique: int = 20) -> bool:
    """
    Determine whether a column should be modeled as categorical.

    Parameters
    ----------
    series : pd.Series
        Input column.

    max_unique : int, default=20
        Maximum number of unique values to treat as categorical-like.

    Returns
    -------
    bool
        True if the column should be treated as categorical.
    """
    non_null = series.dropna()
    if non_null.empty:
        return True

    if pd.api.types.is_bool_dtype(series):
        return True

    if is_small_categorical_integer(series, max_unique):
        return True

    if is_small_datetime(series, max_unique):
        return True

    return not (
        pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series)
    )


def is_continuous(series: pd.Series, max_unique: int = 20) -> bool:
    """
    Determine whether a column should be modeled as continuous.

    Parameters
    ----------
    series : pd.Series
        Input column.

    max_unique : int, default=20
        Maximum number of unique values to treat as categorical-like.

    Returns
    -------
    bool
        True if the column should be treated as continuous.
    """
    return not is_categorical(series, max_unique)
