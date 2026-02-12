import pandas as pd
from csvw_safe_library.metadata import generate_csvw_dp_metadata
from csvw_safe_library.dummy import make_dummy_dataset_csvw_dp
from csvw_safe_library.assert_structure import assert_same_structure

def test_dummy_generation():
    df = pd.read_csv("tests/sample_data.csv")
    metadata = generate_csvw_dp_metadata(df, csv_url="tests/sample_data.csv", individual_col="user_id")

    dummy_df = make_dummy_dataset_csvw_dp(metadata, nb_rows=10, seed=42)

    # Basic shape check
    assert dummy_df.shape[1] == df.shape[1]
    assert dummy_df.shape[0] == 10

    # Structural check
    assert_same_structure(df, dummy_df, check_categories=False)