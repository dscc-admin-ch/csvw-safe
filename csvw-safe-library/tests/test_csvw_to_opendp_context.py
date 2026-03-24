import opendp.prelude as dp
import polars as pl
import pytest

from csvw_safe.constants import COL_LIST, COL_NAME, DATATYPE, MAX_CONTRIB, PRIVACY_ID
from csvw_safe.csvw_to_opendp_context import csvw_to_opendp_context

dp.enable_features("contrib")


@pytest.fixture
def mock_csvw_meta():
    """Return a small CSVW-SAFE metadata dict for testing."""
    return {
        MAX_CONTRIB: 3,
        COL_LIST: [
            {COL_NAME: "user_id", DATATYPE: "integer", PRIVACY_ID: True},
            {COL_NAME: "age", DATATYPE: "integer"},
            {COL_NAME: "signup_date", DATATYPE: "datetime"},
        ],
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


# --------------------------------------------------
# Tests
# --------------------------------------------------
def test_epsilon_context(mock_csvw_meta, mock_data):
    """Test OpenDP context creation with epsilon (Laplace DP)."""
    epsilon = 1.0
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta,
        data=mock_data,
        epsilon=epsilon,
        delta=1e-6,
    )
    assert context is not None


def test_rho_context(mock_csvw_meta, mock_data):
    """Test OpenDP context creation with rho (Gaussian DP)."""
    rho = 0.5
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta,
        data=mock_data,
        rho=rho,
    )
    assert context is not None


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
    with pytest.raises(ValueError, match="Missing required field"):
        csvw_to_opendp_context(
            csvw_meta=meta_missing,
            data=mock_data,
            epsilon=1.0,
        )


def test_split_evenly_over(mock_csvw_meta, mock_data):
    """Test split_evenly_over parameter."""
    context = csvw_to_opendp_context(
        csvw_meta=mock_csvw_meta,
        data=mock_data,
        epsilon=0.5,
        split_evenly_over=5,
    )
