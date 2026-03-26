import pandas as pd

from csvw_safe.constants import DependencyType
from csvw_safe.make_metadata_from_data import (
    get_continuous_bounds,
    identify_dependency,
)


def test_get_continuous_bounds_numeric():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    min_val, max_val = get_continuous_bounds(df["a"])
    assert min_val == 1
    assert max_val == 5


def test_get_continuous_bounds_datetime():
    datetime_df = pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=5)})
    min_val, max_val = get_continuous_bounds(datetime_df["ts"])
    assert min_val == datetime_df["ts"].min().isoformat()
    assert max_val == datetime_df["ts"].max().isoformat()


def test_identify_dependency_bigger_overlap():
    # bounds overlap
    df = pd.DataFrame(
        {
            "a": list(range(30, 60)),  # target column
            "b": list(range(25, 55)),  # depends_on column, overlaps bounds
        }
    )
    result = identify_dependency(df, "a")
    assert any(d.depends_on == "b" and d.dependency_type == DependencyType.BIGGER for d in result)

    result = identify_dependency(df, "b")
    assert result == []


def test_identify_dependency_bigger_no_overlap():
    """
    Test numeric 'BIGGER' dependency when bounds do NOT overlap.
    """
    df = pd.DataFrame(
        {
            "a": list(range(30, 60)),  # target column
            "b": list(range(0, 30)),  # depends_on column, no overlap
        }
    )

    result = identify_dependency(df, "a")

    # No dependency should be detected
    assert all(d.depends_on != "b" for d in result)


def test_identify_dependency_bigger_equal_bounds():
    """
    Test numeric 'BIGGER' when some values are equal.
    """
    df = pd.DataFrame(
        {
            "a": list(range(30, 60)),
            "b": list(range(30, 60)),  # identical values → a >= b
        }
    )

    result = identify_dependency(df, "a")

    assert any(d.depends_on == "b" and d.dependency_type == DependencyType.BIGGER for d in result)


def test_identify_dependency_fixed():
    df = pd.DataFrame(
        {
            "target": [100, 200, 300, 400],
            "id": ["A", "B", "C", "D"],  # Each key has exactly one target value
        }
    )

    result = identify_dependency(df, "target")

    # There should be one dependency: id -> target, type FIXED
    fixed_deps = [
        d for d in result if d.depends_on == "id" and d.dependency_type == DependencyType.FIXED
    ]

    assert len(fixed_deps) == 1
    dep = fixed_deps[0]
    assert dep.depends_on == "id"
    assert dep.dependency_type == DependencyType.FIXED


def test_identify_dependency_mapping():
    df = pd.DataFrame(
        {
            "target": [1, 2, 2, 3, 3, 4, 4],
            "key": ["A", "A", "B", "B", "C", "C", "C"],  # multiple target values per key
        }
    )

    # max_mapping_values = 2 → any key mapping >2 values will be dropped
    result = identify_dependency(df, "target", max_mapping_keys=5, max_mapping_values=2)

    # Find MAPPING dependency
    mapping_deps = [
        d for d in result if d.depends_on == "key" and d.dependency_type == DependencyType.MAPPING
    ]

    assert len(mapping_deps) == 1
    dep = mapping_deps[0]

    # The value_map should include only keys where #values <= max_mapping_values
    expected_mapping = {
        "A": [1, 2],
        "B": [2, 3],
        "C": [3, 4],  # truncated to max_mapping_values=2
    }
    assert dep.value_map == expected_mapping


def test_identify_dependency_exceeds_max_keys_fixed():
    # 10 rows
    n_rows = 10
    max_keys = 4

    # 'x' is target, 'y' has repeated values to avoid fixed dependency
    df = pd.DataFrame(
        {
            "x": list(range(n_rows)),
            "y": [0, 0, 1, 1, 2, 2, 3, 3, 4, 4],  # 5 unique keys ≤ max_keys
        }
    )

    # Set max_mapping_keys smaller to trigger the branch
    deps = identify_dependency(df, "x", max_mapping_keys=max_keys)

    # 'y' should be skipped because n_keys=5 > max_mapping_keys=4
    assert deps == []
