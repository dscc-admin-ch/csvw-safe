import numpy as np
import pandas as pd

def make_dummy_dataset_csvw_dp(metadata: dict, nb_rows: int = 100, seed: int = 0) -> pd.DataFrame:
    """
    Create a dummy dataset from CSVW-DP metadata.
    """
    rng = np.random.default_rng(seed)
    data_dict = {}

    columns = metadata["tableSchema"]["columns"]

    for col in columns:
        name = col["name"]
        dtype = col["datatype"]
        nullable_prop = col.get("dp:nullableProportion", 0)

        # ---------- STRING ----------
        if dtype == "string":
            categories = col.get("dp:publicPartitions", [])
            if not categories:
                raise ValueError(f"No categories for string column {name}")

            serie = pd.Series(rng.choice(categories, size=nb_rows), dtype="string")

        # ---------- BOOLEAN ----------
        elif dtype == "boolean":
            serie = pd.Series(rng.choice([True, False], size=nb_rows), dtype="boolean")

        # ---------- INTEGER (categorical or continuous) ----------
        elif dtype == "integer":
            if "dp:publicPartitions" in col:
                # categorical integer
                categories = col["dp:publicPartitions"]
                serie = pd.Series(rng.choice(categories, size=nb_rows), dtype="Int64")
            else:
                low = int(col["minimum"])
                high = int(col["maximum"])
                serie = pd.Series(
                    rng.integers(low, high + 1, size=nb_rows),
                    dtype="Int64"
                )

        # ---------- FLOAT ----------
        elif dtype == "double":
            low = float(col["minimum"])
            high = float(col["maximum"])
            serie = pd.Series(
                low + (high - low) * rng.random(size=nb_rows),
                dtype="float64"
            )

        # ---------- DATETIME ----------
        elif dtype == "dateTime":
            dates = pd.date_range(start=col["minimum"], end=col["maximum"])
            serie = pd.Series(rng.choice(dates, size=nb_rows))

        else:
            raise ValueError(f"Unsupported datatype {dtype} for column {name}")

        # ---------- INSERT NULLS ----------
        if nullable_prop > 0:
            n_null = int(nb_rows * nullable_prop)
            if n_null > 0:
                idx = rng.choice(serie.index, size=n_null, replace=False)
                if dtype == "dateTime":
                    serie.loc[idx] = pd.NaT
                else:
                    serie.loc[idx] = pd.NA

        data_dict[name] = serie

    return pd.DataFrame(data_dict)