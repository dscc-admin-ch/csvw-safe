import numpy as np
import pandas as pd

def is_categorical_int(col, max_unique=20):
    if not pd.api.types.is_numeric_dtype(col):
        return False

    non_null = col.dropna()
    if len(non_null) == 0:
        return False

    is_int = (non_null % 1 == 0).all()
    return is_int and non_null.nunique() <= max_unique


def csvw_dtype(col):
    if pd.api.types.is_bool_dtype(col):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(col):
        return "dateTime"
    if pd.api.types.is_numeric_dtype(col):
        return "double"
    return "string"


def compute_max_partition_length(col_data) -> int:
    # An upper bound on the number of records in any one partition.
    # If you don’t know how many records are in the data, you can specify a very loose upper bound,
    # for example, the size of the total population you are sampling from.
    return int(col_data.value_counts().max())


def compute_max_num_partitions(df, col) -> int:
    # An upper bound on the number of distinct partitions.
    return int(df[col].nunique())


def compute_max_influenced_partitions(df, id_col, col) -> int:
    # The greatest number of partitions any one individual can contribute to.
    # = Max number of different partitions (col values) an individual (id_col) appears in
    return int(df.groupby(id_col)[col].nunique().max())


def compute_max_partition_contributions(df, id_col, col) -> int:
    # The greatest number of records an individual may contribute to any one partition.
    # = For each (id, col_value) pair, count number of records and take the max over all such pairs
    return int(df.groupby([id_col, col], observed=True).size().max())


def compute_margins(df, individual_col, col) -> dict:
    # https://docs.opendp.org/en/stable/api/python/opendp.extras.polars.html#opendp.extras.polars.Margin
    margin_col_info = {}

    col_data = df[col].dropna()
    margin_col_info["max_partition_length"] = compute_max_partition_length(col_data)

    margin_col_info["max_num_partitions"] = compute_max_num_partitions(df, col)
    margin_col_info["max_influenced_partitions"] = compute_max_influenced_partitions(df, individual_col, col)
    margin_col_info["max_partition_contributions"] = compute_max_partition_contributions(df, individual_col, col)

    return margin_col_info

def generate_csvw_dp_metadata(
    df: pd.DataFrame,
    csv_url: str,
    individual_col: str,
    max_contributions: int = 2,
):
    CSVW_DP_CONTEXT = [
        "http://www.w3.org/ns/csvw",
        "https://w3id.org/csvw-dp#"
    ]
    meta = {
        "@context": CSVW_DP_CONTEXT,
        "url": csv_url,
        "tableSchema": {
            "dp:maxContributions": int(max_contributions),
            "dp:maxTableLength": int(len(df)),
            "dp:tableLength": int(len(df)),
            "columns": []
        }
    }

    for col in df.columns:
        col_data = df[col]
        nullable_prop = float(col_data.isna().mean())

        col_info = {
            "name": col,
            "datatype": csvw_dtype(col_data),
            "dp:privacyId": col == individual_col,
            "required": nullable_prop == 0,
            "dp:nullableProportion": round(nullable_prop, 2),
        }

        non_null = col_data.dropna()

        # ---------- Numeric ----------
        if pd.api.types.is_numeric_dtype(non_null):

            if is_categorical_int(non_null):
                # treat as categorical
                categories = sorted(non_null.astype(int).unique().tolist())
                col_info["datatype"] = "integer"
                col_info["dp:publicPartitions"] = categories
        
                margins = compute_margins(df, individual_col, col)
        
                col_info.update({
                    "dp:maxNumPartitions": margins["max_num_partitions"],
                    "dp:maxPartitionLength": margins["max_partition_length"],
                    "dp:maxInfluencedPartitions": margins["max_influenced_partitions"],
                    "dp:maxPartitionContribution": margins["max_partition_contributions"],
                })
        
            else:
                # continuous numeric
                col_info["datatype"] = "double"
                col_info["minimum"] = float(np.floor(non_null.min()))
                col_info["maximum"] = float(np.ceil(non_null.max()))

        # ---------- Categorical ----------
        elif col_info["datatype"] in ("string", "boolean"):
            categories = sorted(
                non_null.dropna().astype(str).unique().tolist()
            )
        
            if col_info["datatype"] == "boolean":
                col_info["dp:publicPartitions"] = [True, False]
            else:
                col_info["dp:publicPartitions"] = categories

                margins = compute_margins(df, individual_col, col)

                col_info.update({
                    "dp:maxNumPartitions": margins["max_num_partitions"],
                    "dp:maxPartitionLength": margins["max_partition_length"],
                    "dp:maxInfluencedPartitions": margins["max_influenced_partitions"],
                    "dp:maxPartitionContribution": margins["max_partition_contributions"],
                })

        # ---------- Datetime ----------
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            col_info["datatype"] = "dateTime"
            col_info["minimum"] = str(non_null.min())
            col_info["maximum"] = str(non_null.max())
    
        meta["tableSchema"]["columns"].append(col_info)

    return meta
