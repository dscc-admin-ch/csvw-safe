"""
CSVW-SAFE Metadata Generator.

This module generates CSVW-SAFE metadata from a CSV dataset. It automatically
infers column datatypes, detects dependencies, builds partitions for categorical
and numeric attributes, and computes contribution bounds relative to a defined
privacy unit.

The output metadata follows the CSVW and CSVW-SAFE conventions used for
privacy-preserving data synthesis and differential privacy pipelines.
"""

import argparse
import json
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple, cast

import numpy as np
import pandas as pd
import pandas.api.types as ptypes

from csvw_safe import constants as C
from csvw_safe import metadata_structure as S
from csvw_safe.datatypes import infer_xmlschema_datatype, is_categorical
from csvw_safe.utils import sanitize


class ContributionLevel(IntEnum):
    """
    Represents the level at which contribution bounds are applied in CSVW-SAFE metadata.

    Levels:
    - TABLE: global table-level contribution bounds
    - COLUMN: per-column contribution bounds
    - PARTITION: per-partition contribution bounds
    """

    TABLE = 0
    COLUMN = 1
    PARTITION = 2

    @classmethod
    def from_str(cls, value: str) -> "ContributionLevel":
        """
        Convert a string representation into a ContributionLevel enum.

        Parameters
        ----------
        value : str
            One of 'table', 'column', 'partition' (case-insensitive).

        Returns
        -------
        ContributionLevel
            Corresponding enum value.

        Raises
        ------
        ValueError
            If the input string does not match any valid level.
        """
        value = value.lower()
        if value == "table":
            return cls.TABLE
        if value == "column":
            return cls.COLUMN
        if value == "partition":
            return cls.PARTITION
        raise ValueError(f"Invalid contribution level: {value}")

    def __str__(self) -> str:
        """
        Return the lowercase string representation of the contribution level.

        Example: ContributionLevel.PARTITION -> 'partition'
        """
        return self.name.lower()


def get_effective_contrib_level(
    column_name: str,
    fine_contributions_level: Dict[str, str],
    default_contributions_level: str,
) -> ContributionLevel:
    """
    Determine effective contribution level for a column.

    Logic:
      - Take column-specific fine level if it exists, else default.
      - Return the maximum of column and default (table < column < partition).
    """
    fine_level = ContributionLevel.from_str(fine_contributions_level.get(column_name, "table"))
    default_level = ContributionLevel.from_str(default_contributions_level)
    # max ensures that 'partition' overrides 'column' or 'table'
    return max(fine_level, default_level)


# ============================================================
# Column level
# ============================================================
def get_continuous_bounds(series: pd.Series) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Compute minimum and maximum values for continuous columns.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    tuple
        (min_value, max_value)
    """
    non_null = series.dropna()
    if non_null.empty:
        return None, None

    value_min = non_null.min()
    value_max = non_null.max()

    if pd.api.types.is_datetime64_any_dtype(series):
        return value_min.isoformat(), value_max.isoformat()
    return value_min, value_max


def identify_fixed_fields(df: pd.DataFrame, column_name: str, threshold: int = 1) -> List[str]:
    """
    Identify columns that are constant within each value of a target column.

    These columns can be treated as deterministic attributes.

    Parameters
    ----------
    df : pd.DataFrame
    column_name : str
        Column used as grouping reference.
    threshold : int
        Maximum allowed unique values to consider fixed.

    Returns
    -------
    list
        Names of fixed columns.
    """
    grouped = df.groupby(column_name, dropna=False)

    nunique = grouped.nunique(dropna=False)
    fixed_columns = cast(
        List[str],  # for the linter
        nunique.columns[(nunique <= threshold).all()].tolist(),
    )
    if len(fixed_columns) == len(df.columns) - 1:
        return []
    return fixed_columns


def identify_dependency(
    column_name: str,
    df: pd.DataFrame,
    mapping_threshold: float = 0.95,
    coverage_threshold: float = 0.8,
    max_mapping_size: int = 25,
) -> List[S.Dependency]:
    """
    Detect dependencies between columns.

    This includes:
    - inequality relationships
    - deterministic mappings

    Parameters
    ----------
    column_name : str
        Target column.
    df : pd.DataFrame
    mapping_threshold : float
        Minimum deterministic ratio required.
    coverage_threshold : float
        Minimum dataset coverage required.
    max_mapping_size : int
        Maximum allowed mapping size.

    Returns
    -------
    list
        Dependency descriptions.
    """
    results: List[S.Dependency] = []

    for col in df.columns:
        if col == column_name:
            continue

        valid = df[[column_name, col]].dropna()

        if valid.empty:
            continue

        s_valid = valid[column_name]
        o_valid = valid[col]

        # Numeric dependency
        if ptypes.is_numeric_dtype(s_valid) and ptypes.is_numeric_dtype(o_valid):

            if (s_valid >= o_valid).all():
                results.append(
                    S.Dependency(
                        depends_on=col,
                        dependency_type=C.DependencyType.BIGGER,
                    )
                )
                continue

            if (s_valid <= o_valid).all():
                results.append(
                    S.Dependency(
                        depends_on=col,
                        dependency_type=C.DependencyType.SMALLER,
                    )
                )
                continue

        if valid[col].nunique() > max_mapping_size:
            continue

        grouped = valid.groupby(col)[column_name].agg(lambda x: list(pd.unique(x)))
        mapping = grouped.to_dict()

        if not mapping:
            continue

        deterministic_ratio = sum(len(v) == 1 for v in mapping.values()) / len(mapping)
        if deterministic_ratio < mapping_threshold:
            continue

        coverage = valid[col].isin(mapping.keys()).sum() / len(valid)
        if coverage < coverage_threshold:
            continue

        clean_mapping = {k: v[0] if len(v) == 1 else v for k, v in mapping.items()}

        results.append(
            S.Dependency(
                depends_on=col,
                dependency_type=C.DependencyType.MAPPING,
                value_map=clean_mapping,
            )
        )

    return results


# ============================================================
# Make Partitions
# ============================================================
def make_predicate(spec: Dict[str, Any], value: Any) -> S.Predicate:
    """
    Build a Predicate object from a column specification and a partition value.

    Parameters
    ----------
    spec : dict
        Column specification containing "kind" and optionally "is_datetime".
    value : Any
        Partition value, either a category or a numeric interval.

    Returns
    -------
    Predicate
        Dataclass representing the partition predicate.
    """
    if spec["kind"] == "categorical":
        return S.Predicate(partition_value=value)

    # Numeric or datetime interval
    interval = value
    lower = pd.to_datetime(interval.left).isoformat() if spec.get("is_datetime") else float(interval.left)
    upper = pd.to_datetime(interval.right).isoformat() if spec.get("is_datetime") else float(interval.right)
    return S.Predicate(lower_bound=lower, upper_bound=upper)


def make_categorical_partitions(
    df: pd.DataFrame, privacy_unit: str, column_name: str
) -> List[S.SingleColumnPartition]:
    """Generate partitions for a categorical column."""
    partitions_meta = build_partitions(
        df,
        privacy_unit,
        [{"name": column_name, "kind": "categorical"}],
    )
    return [p for p in partitions_meta if isinstance(p, S.SingleColumnPartition)]


def make_numeric_partitions(
    df: pd.DataFrame,
    privacy_unit: str,
    column_name: str,
    bounds: List[Any],
) -> List[S.SingleColumnPartition]:
    """Generate partitions for a numeric column using provided bins."""
    partitions_meta = build_partitions(
        df,
        privacy_unit,
        [
            {
                "name": column_name,
                "kind": "numeric",
                "bins": bounds,
                "is_datetime": pd.api.types.is_datetime64_any_dtype(df[column_name]),
            }
        ],
    )
    return [p for p in partitions_meta if isinstance(p, S.SingleColumnPartition)]


def get_multi_group_partitions(
    df: pd.DataFrame,
    col_group: List[str],
    continuous_partitions: Dict[str, List[Any]],
    privacy_unit: str,
) -> List[S.MultiColumnPartition]:
    """Generate partitions when grouping by multiple columns."""
    specs = []
    for col in col_group:
        if col in continuous_partitions:
            specs.append(
                {
                    "name": col,
                    "kind": "numeric",
                    "bins": continuous_partitions[col],
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
        else:
            specs.append(
                {
                    "name": col,
                    "kind": "categorical",
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
    partitions = build_partitions(df, privacy_unit, specs)
    return [p for p in partitions if isinstance(p, S.MultiColumnPartition)]


def build_partitions(
    df: pd.DataFrame,
    privacy_unit: str,
    column_specs: List[Dict[str, Any]],
) -> List[S.Partition]:
    """
    Build CSVW-SAFE partitions and compute contribution bounds per partition.

    This function groups the dataset according to the provided column
    specifications and calculates metadata required by CSVW-SAFE, including
    maximum partition size and per-privacy-unit contribution bounds.

    Numeric columns are first discretized into bins before grouping.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset.
    privacy_unit : str
        Column name representing the privacy unit (e.g., patient_id).
    column_specs : list of dict
        Specifications describing how each column should be partitioned.

        Each specification must contain:
        - "name": column name
        - "kind": either "categorical" or "numeric"

        Optional keys:
        - "bins": list of numeric or datetime boundaries (for numeric columns)
        - "is_datetime": bool indicating datetime values

        Example
        -------
        [
            {"name": "species", "kind": "categorical"},
            {"name": "age", "kind": "numeric", "bins": [0, 10, 20, 30]}
        ]

    Returns
    -------
    list of dict
        A list of CSVW-SAFE partition metadata objects. Each entry contains:

        - "@type": Partition type
        - "csvw-safe:predicate": partition condition
        - "csvw-safe:bounds.maxLength": maximum rows in partition
        - "csvw-safe:bounds.maxGroupsPerUnit": maximum rows per privacy unit
        - "csvw-safe:bounds.maxContributions": maximum partitions per unit
    """
    df_work = df.copy() if any(spec["kind"] == "numeric" for spec in column_specs) else df

    grouping_columns = []
    influenced_counts = {}

    for spec in column_specs:

        col = spec["name"]

        if spec["kind"] == "categorical":

            grouping_columns.append(col)

            influenced_counts[col] = df.groupby(privacy_unit)[col].nunique(dropna=True)

        elif spec["kind"] == "numeric":

            bins = pd.to_datetime(spec["bins"]) if spec.get("is_datetime") else sorted(spec["bins"])

            binned_col = f"{col}__bin"

            df_work[binned_col] = pd.cut(df_work[col], bins=bins, right=False)

            grouping_columns.append(binned_col)

            influenced_counts[col] = df_work.groupby(privacy_unit)[binned_col].nunique(dropna=True)

        else:
            raise ValueError(f"Unknown column kind {spec['kind']}")

    partitions_meta: List[S.Partition] = []

    for group_key, group_df in df_work.groupby(grouping_columns, dropna=True, observed=True):

        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        per_privacy_unit_contrib = group_df.groupby(privacy_unit).size()
        max_contrib = max(
            int(influenced_counts[spec["name"]].loc[per_privacy_unit_contrib.index].max())
            for spec in column_specs
        )

        if len(column_specs) == 1:
            partitions_meta.append(
                S.SingleColumnPartition(
                    predicate=make_predicate(column_specs[0], group_key[0]),
                    max_length=int(group_df.shape[0]),
                    max_groups_per_unit=int(per_privacy_unit_contrib.max()),
                    max_contributions=max_contrib,
                )
            )
        else:
            partitions_meta.append(
                S.MultiColumnPartition(
                    predicate={
                        spec["name"]: make_predicate(spec, group_key[i])
                        for i, spec in enumerate(column_specs)
                    },
                    max_length=int(group_df.shape[0]),
                    max_groups_per_unit=int(per_privacy_unit_contrib.max()),
                    max_contributions=max_contrib,
                )
            )

    return partitions_meta


def attach_partitions_to_column(
    column_meta: S.ColumnMetadata,
    partitions_meta: List[S.SingleColumnPartition],
    col_contrib_level: ContributionLevel,
) -> S.ColumnMetadata:
    """
    Attach partition metadata to a column.

    If the contribution level is COLUMN (only possible for categorical columns):
      - Aggregate partition bounds to the column level
      - Remove per-partition bounds
      - Store only the list of public partition values

    Otherwise (PARTITION level):
      - Keep full partition metadata.
    """
    if not partitions_meta:
        return column_meta

    # COLUMN level: collapse partition bounds
    if col_contrib_level == ContributionLevel.COLUMN:
        single_partitions = [p for p in partitions_meta if isinstance(p, S.SingleColumnPartition)]
        column_meta.max_length = max(p.max_length for p in single_partitions)
        column_meta.max_groups_per_unit = max(p.max_groups_per_unit for p in single_partitions)
        column_meta.max_contributions = max(p.max_contributions for p in single_partitions)

        # extract categorical values
        partition_values = []
        for p in partitions_meta:
            pred = p.predicate
            if isinstance(pred, S.Predicate) and pred.partition_value is not None:
                partition_values.append(pred.partition_value)

        column_meta.partitions = partition_values  # list[str]
        column_meta.max_num_partitions = len(partition_values)

        return column_meta

    column_meta.partitions = partitions_meta  # List[S.SingleColumnPartition]
    column_meta.max_num_partitions = len(partitions_meta)

    return column_meta


def make_column_groups(
    df: pd.DataFrame,
    column_groups: List[List[str]],
    fine_contributions_level: Dict[str, str],
    default_contributions_level: str,
    continuous_partitions: Dict[str, List[Any]],
    privacy_unit: str,
) -> List[S.ColumnGroupMetadata]:
    """
    Build CSVW-SAFE metadata for column groups.

    A column group represents a set of columns that should be treated jointly
    when defining contribution bounds and partitions. Partitions are computed
    over the joint values of the columns in the group.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.

    column_groups : list[list[str]]
        List of column groups. Each group is a list of column names that
        should be treated jointly.

    fine_contributions_level : dict[str, str]
        Mapping specifying contribution bound levels for specific columns.
        Values must be either ``"column"`` or ``"partition"``.

    default_contributions_level : str
        Default contribution bound level used when a column is not present
        in ``fine_contributions_level``.

    continuous_partitions : dict[str, list[Any]]
        Mapping of numeric column names to bin boundaries used for generating
        partitions.

    privacy_unit : str
        Name of the column representing the privacy unit (e.g., user_id).

    Returns
    -------
    list[dict[str, Any]]
        A list of CSVW-SAFE column group metadata dictionaries including
        partition definitions and contribution bounds.
    """
    column_groups_metadata = []

    for col_group in column_groups:

        for col in col_group:
            col_contrib_level = get_effective_contrib_level(
                col, fine_contributions_level, default_contributions_level
            )
            assert col_contrib_level in [
                ContributionLevel.COLUMN,
                ContributionLevel.PARTITION,
            ]

        partitions_meta = get_multi_group_partitions(
            df,
            col_group,
            continuous_partitions,
            privacy_unit,
        )

        group_meta = S.ColumnGroupMetadata(
            columns=col_group,
            partitions=partitions_meta,
            max_num_partitions=len(partitions_meta),
        )

        column_groups_metadata.append(group_meta)

    return column_groups_metadata


def make_metadata_from_data(
    df: pd.DataFrame,
    privacy_unit: str,
    max_contributions: int = 2,
    continuous_partitions: Optional[Dict[str, List[Any]]] = None,
    column_groups: Optional[List[List[str]]] = None,
    default_contributions_level: str = "table",
    fine_contributions_level: Optional[Dict[str, str]] = None,
) -> Any:
    """
    Generate CSVW-SAFE metadata from a dataset and return JSON-serializable dictionary.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset.
    privacy_unit : str
        Column identifying the privacy unit.
    max_contributions : int
        Maximum number of contributions per unit.
    continuous_partitions : dict, optional
        Numeric partition boundaries.
    column_groups : list, optional
        Column groups to generate joint partitions.
    default_contributions_level : str
        Default contribution level ("table", "column", "partition").
    fine_contributions_level : dict, optional
        Per-column override for contribution level.

    Returns
    -------
    TableMetadata
        CSVW-SAFE metadata structure as a dataclass.
    """
    if privacy_unit not in df.columns:
        raise ValueError(f"Privacy unit column '{privacy_unit}' not found.")

    if fine_contributions_level is None:
        fine_contributions_level = {}

    if continuous_partitions is None:
        continuous_partitions = {}

    # Any column with numeric partitions is treated at partition level
    for col in continuous_partitions:
        fine_contributions_level[col] = "partition"

    if column_groups is None:
        column_groups = []

    columns_meta: List[S.ColumnMetadata] = []

    for column_name in df.columns:
        series = df[column_name]

        # Determine effective contribution level for this column
        col_contrib_level = get_effective_contrib_level(
            column_name, fine_contributions_level, default_contributions_level
        )

        # Infer datatype
        datatype = infer_xmlschema_datatype(series)

        column_meta = S.ColumnMetadata(
            name=column_name,
            datatype=datatype,
            required=series.isna().sum() == 0,
            privacy_id=(column_name == privacy_unit),
            nullable_proportion=np.ceil(series.isna().mean() * 1000) / 1000,
        )

        # Row-level dependencies
        deps = identify_dependency(column_name, df)
        if deps:
            column_meta.dependencies = deps

        # Fixed-per-entity fields
        fixed_fields = identify_fixed_fields(df, column_name)
        if fixed_fields:
            column_meta.fixed_per_entity = fixed_fields

        # Continuous column bounds
        if datatype != "string":
            minimum, maximum = get_continuous_bounds(series)
            column_meta.minimum = minimum
            column_meta.maximum = maximum

        # Partitions for column- or partition-level contributions
        if col_contrib_level != ContributionLevel.TABLE:
            if is_categorical(series):
                partitions_meta = make_categorical_partitions(df, privacy_unit, column_name)
                column_meta = attach_partitions_to_column(column_meta, partitions_meta, col_contrib_level)
            elif column_name in continuous_partitions:
                bounds = sorted(continuous_partitions[column_name])
                partitions_meta = make_numeric_partitions(df, privacy_unit, column_name, bounds)
                column_meta = attach_partitions_to_column(column_meta, partitions_meta, col_contrib_level)

        columns_meta.append(column_meta)

    # Column groups
    groups_meta: Optional[List[S.ColumnGroupMetadata]] = None
    if column_groups:
        groups_meta = make_column_groups(
            df,
            column_groups,
            fine_contributions_level,
            default_contributions_level,
            continuous_partitions,
            privacy_unit,
        )

    # Return strongly-typed TableMetadata
    table_metadata = S.TableMetadata(
        privacy_unit=privacy_unit,
        max_contributions=max_contributions,
        max_length=len(df),
        public_length=len(df),
        columns=columns_meta,
        column_groups=groups_meta,
    )

    # Convert dataclass to dict and sanitize for JSON
    metadata_dict = table_metadata.to_dict()
    return sanitize(metadata_dict)


# ============================================================
# CLI
# ============================================================
def main() -> None:
    """
    Command-line entry point for generating CSVW-SAFE metadata.

    This function parses command-line arguments, loads the input CSV dataset,
    performs basic datatype inference (including datetime detection), and
    generates CSVW-SAFE metadata describing the dataset structure, privacy
    unit, contribution bounds, and optional partitions.

    The resulting metadata is written as a JSON file.

    Command-line arguments
    ----------------------
    csv_file : str
        Path to the input CSV dataset.

    --privacy-unit : str
        Name of the column representing the privacy unit (e.g., patient_id).

    --output : str, optional
        Output JSON file where the generated metadata will be written.
        Default is "metadata.json".

    --max-contributions : int, optional
        Declared global maximum number of contributions per privacy unit
        (L-infinity bound). Default is 2.

    --continuous_partitions : str, optional
        JSON string specifying bin boundaries for continuous columns.

    --column_groups : str, optional
        JSON string specifying groups of columns for joint partitioning.

    --default_contributions_level : {"table", "column", "partition"}, optional
        Default contribution bound level applied to columns.

    --fine_contributions_level : str, optional
        JSON string specifying column-specific contribution levels.

    Notes
    -----
    Datetime inference is attempted automatically for all columns by
    attempting to parse values using ``pandas.to_datetime``.

    The generated metadata conforms to the CSVW-SAFE specification and
    can be used by downstream privacy-preserving data synthesis systems.
    """
    parser = argparse.ArgumentParser(description="Generate CSVW-SAFE metadata from a CSV dataset.")

    parser.add_argument("csv_file", help="Path to input CSV file")

    parser.add_argument(
        "--privacy-unit",
        required=True,
        help="Column defining the privacy unit (e.g., patient_id)",
    )

    parser.add_argument("--output", default="metadata.json", help="Output metadata JSON file")

    parser.add_argument(
        "--max-contributions",
        type=int,
        default=2,
        help="Declared bounds.maxContributions (l_infinity)",
    )

    parser.add_argument(
        "--continuous_partitions",
        type=str,
        default=None,
        help="JSON string of bounds per continuous column",
    )

    parser.add_argument("--column_groups", type=str, default=None, help="JSON string of column groups")

    parser.add_argument(
        "--default_contributions_level",
        type=str,
        default="table",
        choices=["table", "column", "partition"],
        help="One of 'table', 'column', 'partition'",
    )
    parser.add_argument(
        "--fine_contributions_level",
        type=str,
        default=None,
        help="JSON string with column and expected contribution level ('column' or 'partition')",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)
    for col in df.columns:
        try:
            df[col] = pd.to_datetime(df[col])
        except (ValueError, TypeError):
            pass

    continuous_partitions = json.loads(args.continuous_partitions) if args.continuous_partitions else {}
    column_groups = json.loads(args.column_groups) if args.column_groups else []

    metadata = make_metadata_from_data(
        df=df,
        privacy_unit=args.privacy_unit,
        max_contributions=args.max_contributions,
        continuous_partitions=continuous_partitions,
        column_groups=column_groups,
    )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"CSVW-SAFE metadata written to {args.output}")


if __name__ == "__main__":
    main()
