"""CSVW-SAFE DataTypes Utilities."""

from datetime import datetime
from enum import StrEnum
from typing import TypeVar

import pandas as pd


class ColumnKind(StrEnum):
    """Partition Kind."""

    CATEGORICAL = "categorical"
    CONTINUOUS = "continuous"


T = TypeVar("T", int, float, datetime)


class DataTypesGroups(StrEnum):
    """Column types main groups for metadata."""

    STRING = "string"  # categorical
    BOOLEAN = "boolean"  # categorical
    INTEGER = "integer"  # categorical or continuous
    FLOAT = "float"  # categorical or continuous
    DATETIME = "dateTime"  # categorical or continuous
    DURATION = "duration"  # categorical or continuous


class DataTypes(StrEnum):
    """Precise column types for metadata."""

    # String (categorical)
    STRING = "string"

    # Boolean (categorical)
    BOOLEAN = "boolean"

    # Integer (categorical or continuous)
    INTEGER = "integer"
    LONG = "long"
    INT = "int"
    SHORT = "short"
    NON_NEGATIVE_INTEGER = "nonNegativeInteger"
    POSITIVE_INTEGER = "positiveInteger"
    UNSIGNED_LONG = "unsignedLong"
    UNSIGNED_INT = "unsignedInt"
    UNSIGNED_SHORT = "unsignedShort"
    UNSIGNED_BYTE = "unsignedByte"
    NON_POSITIVE_INTEGER = "nonPositiveInteger"
    NEGATIVE_INTEGER = "negativeInteger"

    # FLOAT (categorical or continuous)
    DECIMAL = "decimal"
    DOUBLE = "double"
    FLOAT = "float"

    # DATETIME (categorical or continuous)
    DATE = "date"
    DATETIME = "dateTime"
    DATETIMESTAMP = "dateTimeStamp"

    # DURATION (categorical or continuous)
    DURATION = "duration"
    DAYTIMEDURATION = "dayTimeDuration"
    YEARMONTHDURATION = "yearMonthDuration"


XSD_GROUP_MAP: dict[DataTypes, DataTypesGroups] = {
    # String
    DataTypes.STRING: DataTypesGroups.STRING,
    # Boolean
    DataTypes.BOOLEAN: DataTypesGroups.BOOLEAN,
    # Integers
    DataTypes.INTEGER: DataTypesGroups.INTEGER,
    DataTypes.LONG: DataTypesGroups.INTEGER,
    DataTypes.INT: DataTypesGroups.INTEGER,
    DataTypes.SHORT: DataTypesGroups.INTEGER,
    DataTypes.NON_NEGATIVE_INTEGER: DataTypesGroups.INTEGER,
    DataTypes.POSITIVE_INTEGER: DataTypesGroups.INTEGER,
    DataTypes.UNSIGNED_LONG: DataTypesGroups.INTEGER,
    DataTypes.UNSIGNED_INT: DataTypesGroups.INTEGER,
    DataTypes.UNSIGNED_SHORT: DataTypesGroups.INTEGER,
    DataTypes.UNSIGNED_BYTE: DataTypesGroups.INTEGER,
    DataTypes.NON_POSITIVE_INTEGER: DataTypesGroups.INTEGER,
    DataTypes.NEGATIVE_INTEGER: DataTypesGroups.INTEGER,
    # Floats
    DataTypes.DECIMAL: DataTypesGroups.FLOAT,
    DataTypes.DOUBLE: DataTypesGroups.FLOAT,
    DataTypes.FLOAT: DataTypesGroups.FLOAT,
    # Datetime
    DataTypes.DATE: DataTypesGroups.DATETIME,
    DataTypes.DATETIME: DataTypesGroups.DATETIME,
    DataTypes.DATETIMESTAMP: DataTypesGroups.DATETIME,
    # Duration
    DataTypes.DURATION: DataTypesGroups.DURATION,
    DataTypes.DAYTIMEDURATION: DataTypesGroups.DURATION,
    DataTypes.YEARMONTHDURATION: DataTypesGroups.DURATION,
}


def is_date(value: str) -> bool:
    """Infer if value is a date in YYYY-MM-DD format."""
    if not isinstance(value, str):
        return False
    try:
        _ = datetime.fromisoformat(value)
        return len(value) <= 10  # YYYY-MM-DD only
    except (ValueError, TypeError):
        return False


def is_datetime(value: str) -> bool:
    """Infer if value is a datetime (ISO 8601 format)."""
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False


def refine_integer_type(series: pd.Series) -> DataTypes:
    """Infer type of integer."""
    s = series.dropna()

    if (s >= 0).all():
        if (s > 0).all():
            return DataTypes.POSITIVE_INTEGER
        return DataTypes.NON_NEGATIVE_INTEGER

    if (s <= 0).all():
        if (s < 0).all():
            return DataTypes.NEGATIVE_INTEGER
        return DataTypes.NON_POSITIVE_INTEGER

    return DataTypes.INTEGER


def is_categorical(series: pd.Series, max_unique: int = 20) -> bool:
    """Infer is the series is categorical (by type or number of unique values)."""
    non_null = series.dropna()
    if non_null.empty:
        return True

    inferred = infer_xmlschema_datatype(series)
    group = XSD_GROUP_MAP[inferred]

    # always categorical
    if group in {DataTypesGroups.STRING, DataTypesGroups.BOOLEAN}:
        return True

    # numeric/datetime → depends on cardinality
    if group in {
        DataTypesGroups.INTEGER,
        DataTypesGroups.FLOAT,
        DataTypesGroups.DATETIME,
        DataTypesGroups.DURATION,
    }:
        non_null = series.dropna()
        return bool(non_null.nunique() <= max_unique)

    return True


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


def infer_xmlschema_datatype(  # pylint: disable=too-many-return-statements
    series: pd.Series,
) -> DataTypes:
    """Infer xml schema datatype."""
    s = series.dropna()

    if s.empty:
        return DataTypes.STRING

    # Native pandas types
    if pd.api.types.is_bool_dtype(s):
        return DataTypes.BOOLEAN

    if pd.api.types.is_datetime64_any_dtype(s):
        return DataTypes.DATETIME

    if pd.api.types.is_integer_dtype(s):
        return refine_integer_type(s)

    if pd.api.types.is_float_dtype(s):
        if (s % 1 == 0).all():
            return refine_integer_type(s)
        return DataTypes.DOUBLE

    # String
    if pd.api.types.is_string_dtype(s) or s.dtype == object:
        sample = s.astype(str).head(50)

        if sample.map(lambda v: v.lower() in {"true", "false"}).all():
            return DataTypes.BOOLEAN

        if sample.map(is_date).all():
            return DataTypes.DATE

        if sample.map(is_datetime).all():
            return DataTypes.DATETIME

        # Numeric inside strings
        nums = pd.to_numeric(sample, errors="coerce")
        if not nums.isna().any():
            if (nums % 1 == 0).all():
                return refine_integer_type(nums)
            return DataTypes.DOUBLE
    return DataTypes.STRING


def to_pandas_dtype(csvw_type: DataTypes) -> str:
    """Xml datatype to pandas datatype."""
    if not csvw_type:
        raise ValueError("Missing DataTypes")

    group = XSD_GROUP_MAP[csvw_type]

    if group == DataTypesGroups.INTEGER:
        return "Int64"  # nullable safe

    if group == DataTypesGroups.FLOAT:
        return "float64"

    if group == DataTypesGroups.BOOLEAN:
        return "boolean"

    if group == DataTypesGroups.DATETIME:
        return "datetime64[ns]"

    return "string"


def to_snsql_datatype(csvw_type: DataTypes) -> str:
    """Smartnoise-sql datatype to pandas datatype."""
    if not csvw_type:
        raise ValueError("Missing DataTypes")

    group = XSD_GROUP_MAP[csvw_type]

    if group == DataTypesGroups.INTEGER:
        return "int"

    if group == DataTypesGroups.FLOAT:
        return "float"

    if group == DataTypesGroups.BOOLEAN:
        return "boolean"

    if group == DataTypesGroups.DATETIME:
        return "datetime"

    return "string"
