import pytest
import pandas as pd
import numpy as np
from csvw_safe.make_metadata_from_data import get_continuous_bounds, identify_fixed_fields, identify_dependance

@pytest.fixture
def numeric_df():
    return pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [5, 4, 3, 2, 1],
        "c": [10, 10, 10, 10, 10],
        "d": [1, 2, 2, np.nan, 1]
    })

@pytest.fixture
def datetime_df():
    return pd.DataFrame({
        "ts": pd.date_range("2025-01-01", periods=5),
        "ts2": pd.date_range("2025-01-01", periods=5)[::-1]
    })

@pytest.fixture
def mapping_df():
    return pd.DataFrame({
        "key": ["A", "B", "B", "C", "C"],
        "value": [1, 2, 2, 3, 3],
        "other": ["X", "Y", "Y", "Z", "Z"]
    })

@pytest.fixture
def df_constant_per_group():
    return pd.DataFrame({
        "group": ["A", "A", "B", "B"],
        "fixed1": [10, 10, 20, 20],
        "fixed2": ["X", "X", "Y", "Y"],
        "random": [1, 2, 3, 4]
    })

@pytest.fixture
def df_with_nan():
    return pd.DataFrame({
        "group": ["A", "A", "B", "B"],
        "fixed": [1, 1, 2, 2],
        "maybe": [np.nan, np.nan, np.nan, np.nan],
        "random": [5, 6, 7, 8]
    })


# ==========================
# get_continuous_bounds tests
# ==========================
def test_get_continuous_bounds_numeric(numeric_df):
    min_val, max_val = get_continuous_bounds(numeric_df["a"])
    assert min_val == 1
    assert max_val == 5

def test_get_continuous_bounds_empty():
    s = pd.Series([], dtype=float)
    min_val, max_val = get_continuous_bounds(s)
    assert min_val is None
    assert max_val is None

def test_get_continuous_bounds_datetime(datetime_df):
    min_val, max_val = get_continuous_bounds(datetime_df["ts"])
    assert min_val == datetime_df["ts"].min().isoformat()
    assert max_val == datetime_df["ts"].max().isoformat()

# ==========================
# identify_fixed_fields tests
# ==========================
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

def test_fixed_fields_with_nans(df_with_nan):
    # 'fixed' has <=1 unique per group -> detected
    # 'maybe' has only NaN -> still treated as <= threshold per group
    fixed = identify_fixed_fields(df_with_nan, "group", threshold=1)
    assert "fixed" in fixed
    assert "maybe" in fixed
    assert "random" not in fixed

# ==========================
# identify_dependance tests
# ==========================
def test_identify_dependance_bigger(numeric_df):
    result = identify_dependance("a", numeric_df)

    # Expected dependencies based on numeric_df
    expected = [
        {"csvw-safe:synth.dependsOn": "b", "csvw-safe:synth.how": "monotonic"},
        {"csvw-safe:synth.dependsOn": "c", "csvw-safe:synth.how": "smaller"},
        {"csvw-safe:synth.dependsOn": "d", "csvw-safe:synth.how": "bigger"},
    ]
    
    # Simplify result to only keys we care about
    simplified_result = [
        {k: r[k] for k in ["csvw-safe:synth.dependsOn", "csvw-safe:synth.how"]}
        for r in result
    ]
    
    # Assert all expected dependencies are present
    assert simplified_result == expected

def test_identify_dependance_monotonic():
    df = pd.DataFrame({
        "x": [1, 2, 3, 4, 5],
        "y": [2, 4, 6, 8, 10]
    })
    result = identify_dependance("x", df)
    expected = [{
        "csvw-safe:synth.dependsOn": "y",
        "csvw-safe:synth.how": "monotonic",
        "csvw-safe:synth.correlation": 1.0
    }]
    assert result == expected

def test_identify_dependance_mapping(mapping_df):
    result = identify_dependance("value", mapping_df, mapping_threshold=0.9, coverage_threshold=0.8)
    expected_mapping = {"A": 1, "B": 2, "C": 3}
    mapping_dep = next((r for r in result if r["csvw-safe:synth.how"] == "mapping"), None)
    assert mapping_dep is not None
    assert mapping_dep["csvw-safe:synth.mapping"] == expected_mapping
