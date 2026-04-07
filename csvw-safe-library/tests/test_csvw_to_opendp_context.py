import opendp.prelude as dp
import polars as pl
import pytest

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    EXHAUSTIVE_PARTITIONS,
    MAX_CONTRIB,
    MAX_LENGTH,
    PRIVACY_ID,
    PRIVACY_UNIT,
    TABLE_SCHEMA,
)
from csvw_safe.csvw_to_opendp_context import csvw_to_opendp_context

dp.enable_features("contrib")


@pytest.fixture
def mock_csvw_meta():
    """Return a small CSVW-SAFE metadata dict for testing."""
    return {
        MAX_CONTRIB: 3,
        TABLE_SCHEMA: {
            COL_LIST: [
                {COL_NAME: "user_id", DATATYPE: "integer", PRIVACY_ID: True},
                {COL_NAME: "age", DATATYPE: "integer"},
                {COL_NAME: "signup_date", DATATYPE: "datetime"},
            ],
        },
    }


@pytest.fixture
def mock_data():
    """Return a small Polars LazyFrame for testing."""
    df = pl.DataFrame(
        {
            "user_id": [1, 2, 3],
            "age": [25, 30, 40],
            "signup_date": ["2021-01-01", "2021-06-01", "2022-01-01"],
        }
    )
    return df.lazy()


def test_epsilon_context(mock_csvw_meta, mock_data):
    """Test OpenDP context creation with epsilon (Laplace DP)."""
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta, data=mock_data, epsilon=10.0, delta=1e-6, split_evenly_over=1
    )
    query = context.query().select(dp.len())
    res = query.release().collect()
    assert res.select("len").item() > 0


def test_rho_context(mock_csvw_meta, mock_data):
    """Test OpenDP context creation with rho (Gaussian DP)."""
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta, data=mock_data, rho=0.5, split_evenly_over=1
    )
    query = context.query().select(dp.len())
    res = query.release().collect()
    assert res.select("len").item() > -1000


def test_either_required(mock_csvw_meta, mock_data):
    """Ensure ValueError is raised if neither epsilon nor rho is provided."""
    with pytest.raises(ValueError, match="Either epsilon or rho must be provided"):
        csvw_to_opendp_context(
            csvw_meta=mock_csvw_meta,
            data=mock_data,
        )


def test_missing_max_contrib(mock_data):
    """Ensure ValueError is raised if max_contributions is missing."""
    meta_missing = {"columns": [{COL_NAME: "user_id", DATATYPE: "integer"}]}
    with pytest.raises(ValueError, match="Missing max_contributions in metadata"):
        csvw_to_opendp_context(
            csvw_meta=meta_missing,
            data=mock_data,
            epsilon=1.0,
        )


def test_split_evenly_over(mock_csvw_meta, mock_data):
    """Test split_evenly_over parameter."""
    mock_csvw_meta[MAX_LENGTH] = 10  # second query
    mock_csvw_meta[TABLE_SCHEMA][COL_LIST][2][EXHAUSTIVE_PARTITIONS] = True  # third query
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta,
        data=mock_data,
        epsilon=5,
        split_evenly_over=4,
    )

    # First query (global count)
    query = context.query().select(dp.len())
    res = query.release().collect()
    assert res is not None

    # Second query (global sum)
    query = context.query().select(pl.col("age").dp.sum(bounds=(0, 100)))
    res = query.release().collect()
    assert res is not None

    # Third query (grouped count)
    query = context.query().group_by("signup_date").agg(dp.len())
    res = query.release().collect()
    assert res is not None

    # Fourth query (grouped sum)
    query = context.query().group_by("signup_date").agg(pl.col.age.dp.sum(bounds=(0, 100)))
    res = query.release().collect()
    assert res is not None

    # Fifth query (should error)
    with pytest.raises(ValueError, match="Privacy allowance has been exhausted"):
        query = context.query().select(dp.len())
        res = query.release().collect()


def test_identifier_context(mock_csvw_meta, mock_data):
    """Test OpenDP context creation with identifier."""
    mock_csvw_meta[PRIVACY_UNIT] = "user_id"
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta, data=mock_data, epsilon=10.0, delta=1e-6, split_evenly_over=1
    )
    assert context is not None


# def test_context_init_split_by_weights(mock_csvw_meta, mock_data):
#     context = csvw_to_opendp_context(
#         csvw_meta=mock_csvw_meta, data=mock_data, epsilon=3, split_by_weights=[1, 1, 1]
#     )

#     assert context.remaining_privacy_loss() == [1.0, 1.0, 1.0]


def test_changes_distance(mock_csvw_meta, mock_data):
    """Test other distance types in get_privacy_unit."""
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta,
        data=mock_data,
        epsilon=1.0,
        split_evenly_over=1,
        distance="changes",
    )

    query = context.query().select(dp.len())
    res = query.release().collect()

    assert res.select("len").item() is not None
