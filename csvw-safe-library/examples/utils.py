import numpy as np
import pandas as pd

def make_random_unique_id(
    df: pd.DataFrame, id_column: str, fixed_fields: list[str], max_contributions: int
):
    """
    Maximum max_contributions contribution per id.
    A same id fixed_fields are the same through their life.
    """
    # Step 1: unique id per row
    df[id_column] = np.arange(len(df))
    
    # Step 2: random grouping inside each (species, island, sex)
    def random_merge(group, max_size=max_contributions):
        idx = group.index.to_numpy()
        np.random.shuffle(idx)
    
        # split into chunks of size <= max_size
        chunks = np.array_split(idx, np.ceil(len(idx) / max_size))
    
        # assign same id inside each chunk
        for chunk in chunks:
            df.loc[chunk, id_column] = chunk[0]
    
    df.groupby(fixed_fields, dropna=False, group_keys=False).apply(random_merge)

    assert df.groupby(id_column).size().max() == max_contributions
    return df