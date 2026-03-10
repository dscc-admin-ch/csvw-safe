import numpy as np
import pandas as pd
import pytest

from csvw_safe import constants as C
from csvw_safe.make_metadata_from_data import (
    get_continuous_bounds,
    identify_dependency,
    identify_fixed_fields,
)


def test_get_continuous_bounds_numeric():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    min_val, max_val = get_continuous_bounds(df["a"])
    assert min_val == 1
    assert max_val == 5


def test_get_continuous_bounds_empty():
    s = pd.Series([], dtype=float)
    min_val, max_val = get_continuous_bounds(s)
    assert min_val is None
    assert max_val is None


def test_get_continuous_bounds_datetime():
    datetime_df = pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=5)})
    min_val, max_val = get_continuous_bounds(datetime_df["ts"])
    assert min_val == datetime_df["ts"].min().isoformat()
    assert max_val == datetime_df["ts"].max().isoformat()


@pytest.fixture
def df_constant_per_group():
    return pd.DataFrame(
        {
            "group": ["A", "A", "B", "B"],
            "fixed1": [10, 10, 20, 20],
            "fixed2": ["X", "X", "Y", "Y"],
            "random": [1, 2, 3, 4],
        }
    )


def test_fixed_fields_detected(df_constant_per_group):
    # fixed1 and fixed2 have <=1 unique value per group -> should be detected
    fixed = identify_fixed_fields(df_constant_per_group, "group", threshold=1)
    assert "fixed1" in fixed
    assert "fixed2" in fixed
    assert "random" not in fixed


def test_no_fixed_fields_for_random(df_constant_per_group):
    # group by random column -> no other column should be fixed
    fixed = identify_fixed_fields(df_constant_per_group, "random", threshold=1)
    assert fixed == []


def test_fixed_fields_with_nans():
    df_with_nan = pd.DataFrame(
        {
            "group": ["A", "A", "B", "B"],
            "fixed": [1, 1, 2, 2],
            "maybe": [np.nan, np.nan, np.nan, np.nan],
            "random": [5, 6, 7, 8],
        }
    )
    # 'fixed' has <=1 unique per group -> detected
    # 'maybe' has only NaN -> still treated as <= threshold per group
    fixed = identify_fixed_fields(df_with_nan, "group", threshold=1)
    assert "fixed" in fixed
    assert "maybe" in fixed
    assert "random" not in fixed


def test_identify_dependency_bigger():
    numeric_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [5, 4, 3, 2, 1],
            "c": [10, 10, 10, 10, 10],
            "d": [1, 2, 2, np.nan, 1],
        }
    )
    result = identify_dependency("a", numeric_df, max_mapping_size=3)

    expected = [
        {C.DEPENDS_ON: "c", C.DEPENDENCY_TYPE: C.DependencyType.SMALLER},
        {C.DEPENDS_ON: "d", C.DEPENDENCY_TYPE: C.DependencyType.BIGGER},
    ]

    simplified_result = [{C.DEPENDS_ON: r.depends_on, C.DEPENDENCY_TYPE: r.dependency_type} for r in result]
    assert simplified_result == expected


def test_identify_dependency_mapping():
    mapping_df = pd.DataFrame(
        {
            "key": ["A", "B", "B", "C", "C"],
            "value": [1, 2, 2, 3, 3],
            "other": ["X", "Y", "Y", "Z", "Z"],
        }
    )

    result = identify_dependency("value", mapping_df, mapping_threshold=0.9, coverage_threshold=0.8)
    expected_mapping = {"A": 1, "B": 2, "C": 3}
    mapping_dep = next((r for r in result if r.dependency_type == C.DependencyType.MAPPING), None)
    assert mapping_dep is not None
    assert mapping_dep.value_map == expected_mapping
