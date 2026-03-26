import numpy as np
import pandas as pd
import pytest

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    MAXIMUM,
    MINIMUM,
    TABLE_SCHEMA,
)
from csvw_safe.datatypes import DataTypes
from csvw_safe.make_dummy_from_metadata import (
    apply_nulls_serie,
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
