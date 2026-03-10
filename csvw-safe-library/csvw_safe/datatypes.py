"""CSVW-SAFE Datatypes Utilities."""

from typing import Any, Dict, Union

import pandas as pd

# Allowed datatypes
VALID_TYPES = {"string", "boolean", "decimal", "double", "dateTime"}

NumericType = Union[int, float]


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


def infer_xmlschema_datatype(series: pd.Series) -> str:
    """
    Infer an XML Schema (XSD 1.1) datatype for a pandas column.

    Parameters
    ----------
    series : pd.Series
        Input column.

    Returns
    -------
    str
        Inferred XML Schema datatype ('string', 'boolean', 'integer', 'double', or 'dateTime').
    """
    s = series.dropna()
    if s.empty:
        return "string"

    dtype_checks = [
        (pd.api.types.is_bool_dtype, "boolean"),
        (pd.api.types.is_datetime64_any_dtype, "dateTime"),
        (pd.api.types.is_integer_dtype, "integer"),
        (pd.api.types.is_float_dtype, "double"),  # special handling below
        (pd.api.types.is_numeric_dtype, "integer"),
    ]

    for check, dtype in dtype_checks:
        if check(s):
            if dtype == "double" and (s % 1 == 0).all():
                return "integer"
            return dtype

    return "string"


def is_categorical(series: pd.Series) -> bool:
    """
    Determine whether a column should be modeled as categorical.

    Parameters
    ----------
    series : pd.Series
        Input column.

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

    if is_small_categorical_integer(series):
        return True

    if is_small_datetime(series):
        return True

    return not (
        pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series)
    )


def map_validator_type(datatype: Any, col_meta: Dict[str, Any]) -> str:
    """Map generator datatypes to validator-compatible types."""
    dtype_str = str(datatype) if datatype is not None else "string"
    if dtype_str == "decimal":
        minimum = col_meta.get("minimum")
        maximum = col_meta.get("maximum")
        if isinstance(minimum, (int, float)) and isinstance(maximum, (int, float)):
            if float(minimum).is_integer() and float(maximum).is_integer():
                return "decimal"
        return "double"
    if dtype_str in VALID_TYPES:
        return dtype_str
    return "string"
