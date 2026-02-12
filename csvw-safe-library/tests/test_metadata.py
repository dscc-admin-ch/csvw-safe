import pandas as pd
from csvw_safe_library.metadata import generate_csvw_dp_metadata

def test_generate_metadata():
    df = pd.read_csv("tests/sample_data.csv")
    metadata = generate_csvw_dp_metadata(df, csv_url="tests/sample_data.csv", individual_col="user_id")
    
    assert "tableSchema" in metadata
    assert "columns" in metadata["tableSchema"]
    assert len(metadata["tableSchema"]["columns"]) == df.shape[1]
    
    # Check individual_col marked as privacyId
    user_col = next(c for c in metadata["tableSchema"]["columns"] if c["name"] == "user_id")
    assert user_col["dp:privacyId"] is True