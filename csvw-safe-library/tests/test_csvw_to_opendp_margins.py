from csvw_safe.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    COLUMNS_IN_GROUP,
    EXHAUSTIVE_PARTITIONS,
    MAX_CONTRIB,
    MAX_GROUPS,
    MAX_LENGTH,
    MAX_NUM_PARTITIONS,
    PUBLIC_LENGTH,
    TABLE_SCHEMA,
)
from csvw_safe.csvw_to_opendp_margins import csvw_to_opendp_margins


def mock_csvw_metadata():
    """CSVW-SAFE metadata for margins testing."""
    return {
        MAX_LENGTH: 1000,
        PUBLIC_LENGTH: 1000,
        TABLE_SCHEMA: {
            COL_LIST: [
                {COL_NAME: "age", MAX_GROUPS: 120, PUBLIC_LENGTH: 500},
                {COL_NAME: "income", MAX_LENGTH: 10, MAX_NUM_PARTITIONS: 40},
                {
                    COL_NAME: "city",
                    MAX_GROUPS: 50,
                    EXHAUSTIVE_PARTITIONS: True,
                },
            ],
        },
    }


def find_margin(margins, by):
    """Helper to find a margin by its grouping columns."""
    for m in margins:
        if getattr(m, "by", []) == by:
            return m
    return None


def test_global_margin():
    """Test global margin creation."""
    csvw_meta = mock_csvw_metadata()
    margins = csvw_to_opendp_margins(csvw_meta)

    global_margin = find_margin(margins, [])

    assert global_margin is not None
    assert getattr(global_margin, "max_length", None) == 1000
    assert getattr(global_margin, "invariant", None) == "lengths"


def test_column_max_groups():
    """Test column-level max_groups mapping."""
    csvw_meta = mock_csvw_metadata()
    margins = csvw_to_opendp_margins(csvw_meta)

    age_margin = find_margin(margins, ["age"])

    assert age_margin is not None
    assert getattr(age_margin, "max_groups", None) == 120

    income_margin = find_margin(margins, ["income"])

    assert income_margin is not None
    assert getattr(income_margin, "max_groups", None) == 40


def test_column_max_length():
    """Test column-level max_length mapping."""
    csvw_meta = mock_csvw_metadata()
    margins = csvw_to_opendp_margins(csvw_meta)

    income_margin = find_margin(margins, ["income"])

    assert income_margin is not None
    assert getattr(income_margin, "max_length", None) == 10


def test_invariant_keys():
    """Test PUBLIC_KEYS → invariant='keys'."""
    csvw_meta = mock_csvw_metadata()
    margins = csvw_to_opendp_margins(csvw_meta)

    city_margin = find_margin(margins, ["city"])

    assert city_margin is not None
    assert getattr(city_margin, "invariant", None) == "keys"

    age_margin = find_margin(margins, ["age"])

    assert age_margin is not None
    assert getattr(age_margin, "invariant", None) == "lengths"


def test_no_optional_fields():
    """Test minimal metadata still produces valid margins."""
    csvw_meta = {
        MAX_CONTRIB: 10,
        TABLE_SCHEMA: {COL_LIST: [{COL_NAME: "col1"}]},
    }

    margins = csvw_to_opendp_margins(csvw_meta)

    # Should still have a column margin
    col_margin = find_margin(margins, ["col1"])
    assert col_margin is not None

    # No max_length or max_groups
    assert getattr(col_margin, "max_length", None) is None
    assert getattr(col_margin, "max_groups", None) is None


def test_column_group():
    """CSVW-SAFE metadata for margins testing."""
    csvw_meta = {
        MAX_CONTRIB: 10,
        TABLE_SCHEMA: {COL_LIST: [{COL_NAME: "col1"}]},
        ADD_INFO: [
            {
                COLUMNS_IN_GROUP: ["species", "island"],
                EXHAUSTIVE_PARTITIONS: True,
            },
        ],
    }
    margins = csvw_to_opendp_margins(csvw_meta)

    # Should have a margin on both columns
    cols_margin = find_margin(margins, ["species", "island"])
    assert cols_margin is not None

    # No max_length or max_groups
    assert getattr(cols_margin, "invariant", None) == "keys"
