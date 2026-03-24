import pandas as pd
import pytest

from csvw_safe.assert_same_structure import assert_same_structure


def test_assert_same_structure():
    df1 = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
    df2 = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
    # should pass
    assert_same_structure(df1, df2, check_categories=False)
    assert_same_structure(df1, df2, check_categories=True)


def test_assert_same_structure_only():
    # Original DataFrame
    df1 = pd.DataFrame(
        {"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 22], "is_adult": [True, True, True]}
    )

    # Dummy DataFrame with compatible structure
    df2 = pd.DataFrame(
        {"name": ["X", "Y", "Z"], "age": [20, 21, 22], "is_adult": [True, False, True]}
    )

    # Should not raise if not checking categories
    assert_same_structure(df1, df2, check_categories=False)

    with pytest.raises(
        AssertionError, match=r"Column 'name' dummy values .* are not subset of original .*"
    ):
        assert_same_structure(df1, df2, check_categories=True)


def test_assert_same_structure_column_order_fail():
    df1 = pd.DataFrame({"a": [1], "b": [2]})
    df2 = pd.DataFrame({"b": [2], "a": [1]})

    with pytest.raises(AssertionError, match="Column names/order differ"):
        assert_same_structure(df1, df2)


def test_assert_same_structure_dtype_fail():
    df1 = pd.DataFrame({"x": [1, 2, 3]})
    df2 = pd.DataFrame({"x": ["a", "b", "c"]})  # string vs integer

    with pytest.raises(AssertionError, match="dtype mismatch"):
        assert_same_structure(df1, df2)


def test_assert_same_structure_nullability_fail():
    df1 = pd.DataFrame({"x": [1, 2, 3]})
    df2 = pd.DataFrame({"x": [1, None, 3]})  # introduces null

    with pytest.raises(AssertionError, match="nullability mismatch"):
        assert_same_structure(df1, df2)


def test_assert_same_structure_category_fail():
    df1 = pd.DataFrame({"color": ["red", "green", "blue"]})
    df2 = pd.DataFrame({"color": ["red", "yellow"]})  # 'yellow' not in original

    with pytest.raises(AssertionError, match="dummy values .* are not subset"):
        assert_same_structure(df1, df2)


def test_assert_same_structure_check_categories_false():
    df1 = pd.DataFrame({"x": [1, 2, 3]})
    df2 = pd.DataFrame({"x": [4, 5, 6]})

    # check_categories=False disables subset check
    assert_same_structure(df1, df2, check_categories=False)
