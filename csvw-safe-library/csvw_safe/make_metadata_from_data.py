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
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from csvw_safe.constants import DependencyType
from csvw_safe.datatypes import (
    ColumnKind,
    DataTypes,
    T,
    infer_xmlschema_datatype,
    is_categorical,
    is_continuous,
)
from csvw_safe.metadata_structure import (
    CategoricalPredicate,
    ColumnGroupMetadata,
    ColumnMetadata,
    ContinuousPredicate,
    Dependency,
    MultiColumnPartition,
    Partition,
    Predicate,
    SingleColumnPartition,
    TableMetadata,
    full_partition_to_key_multi,
    full_partition_to_key_single,
)
from csvw_safe.utils import ContributionLevel, sanitize


def get_effective_contrib_level(
    column_name: str,
    fine_contributions_level: Dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
) -> ContributionLevel:
    """
    Determine effective contribution level for a column.

    Logic:
      - Take column-specific fine level if it exists, else default.
      - Return the maximum of column and default (table < column < partition).
    """
    fine_level = fine_contributions_level.get(column_name, ContributionLevel.TABLE)
    return max(fine_level, default_contributions_level)


def get_continuous_bounds(series: pd.Series) -> tuple[T, T]:
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
    value_min = series.min()
    value_max = series.max()

    if pd.api.types.is_datetime64_any_dtype(series):
        return value_min.isoformat(), value_max.isoformat()
    return value_min, value_max


def identify_dependency(  # pylint: disable=too-many-locals
    df: pd.DataFrame,
    column_name: str,
    max_mapping_keys: int = 25,
    max_mapping_values: int = 10,
) -> List[Dependency]:
    """
    Detect dependencies between columns.

    This includes:
    - inequality relationships
    - deterministic mappings

    Parameters
    ----------
    df : pd.DataFrame
    column_name : str
        Target column.
    max_mapping_keys : int
        Maximum allowed keys in mapping.
    max_mapping_values : int
        Maximum allowed values in a key in a mapping.

    Returns
    -------
    list
        Dependency descriptions.
    """
    results: List[Dependency] = []
    for col in df.columns:
        if col == column_name:
            continue

        valid = df[[column_name, col]].dropna()
        s_valid = valid[column_name]
        o_valid = valid[col]

        # Numeric dependency
        if is_continuous(s_valid) and is_continuous(o_valid):
            if is_datetime64_any_dtype(s_valid) != is_datetime64_any_dtype(o_valid):
                continue
            s_min, s_max = get_continuous_bounds(s_valid)
            o_min, o_max = get_continuous_bounds(o_valid)

            # if bounds overlap only
            if max(s_min, o_min) < min(s_max, o_max):
                if (s_valid >= o_valid).all():
                    results.append(
                        Dependency(depends_on=col, dependency_type=DependencyType.BIGGER)
                    )
                    continue
        else:
            grouped = valid.groupby(col)[column_name].apply(lambda x: list(pd.unique(x)))
            lengths = grouped.str.len()

            # Only if private id (?)
            if (lengths == 1).all():
                results.append(Dependency(depends_on=col, dependency_type=DependencyType.FIXED))
                continue

            # Categorical dependency: finite key and values cardinality
            n_keys = valid[col].nunique()
            if n_keys > max_mapping_keys:
                continue

            mapping = grouped[lengths <= max_mapping_values].to_dict()
            if mapping:
                # Prevent redundant reverse mapping:
                n_values = valid[column_name].nunique()
                if n_keys <= n_values:
                    results.append(
                        Dependency(
                            depends_on=col,
                            dependency_type=DependencyType.MAPPING,
                            value_map=mapping,
                        )
                    )

    return results


def make_predicate(spec: Dict[str, Any], value: Any) -> Predicate:
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
    if spec["kind"] == ColumnKind.CATEGORICAL:
        return CategoricalPredicate(partition_value=value)

    # Numeric or datetime interval
    interval = value
    lower = (
        pd.to_datetime(interval.left).isoformat()
        if spec.get("is_datetime")
        else float(interval.left)
    )
    upper = (
        pd.to_datetime(interval.right).isoformat()
        if spec.get("is_datetime")
        else float(interval.right)
    )
    return ContinuousPredicate(lower_bound=lower, upper_bound=upper)


def make_categorical_partitions(
    df: pd.DataFrame, privacy_unit: str, column_name: str
) -> List[SingleColumnPartition]:
    """Generate partitions for a categorical column."""
    partitions_meta = build_partitions(
        df,
        privacy_unit,
        [{"name": column_name, "kind": ColumnKind.CATEGORICAL}],
    )
    return [p for p in partitions_meta if isinstance(p, SingleColumnPartition)]


def make_numeric_partitions(
    df: pd.DataFrame,
    privacy_unit: str,
    column_name: str,
    bounds: List[Any],
) -> List[SingleColumnPartition]:
    """Generate partitions for a numeric column using provided bins."""
    partitions_meta = build_partitions(
        df,
        privacy_unit,
        [
            {
                "name": column_name,
                "kind": ColumnKind.CONTINUOUS,
                "bins": bounds,
                "is_datetime": pd.api.types.is_datetime64_any_dtype(df[column_name]),
            }
        ],
    )
    return [p for p in partitions_meta if isinstance(p, SingleColumnPartition)]


def get_multi_group_partitions(
    df: pd.DataFrame,
    col_group: List[str],
    continuous_partitions: Dict[str, List[Any]],
    privacy_unit: str,
) -> List[MultiColumnPartition]:
    """Generate partitions when grouping by multiple columns."""
    specs = []
    for col in col_group:
        if col in continuous_partitions:
            specs.append(
                {
                    "name": col,
                    "kind": ColumnKind.CONTINUOUS,
                    "bins": continuous_partitions[col],
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
        else:
            specs.append(
                {
                    "name": col,
                    "kind": ColumnKind.CATEGORICAL,
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
    partitions = build_partitions(df, privacy_unit, specs)
    return [p for p in partitions if isinstance(p, MultiColumnPartition)]


def build_partitions(
    df: pd.DataFrame,
    privacy_unit: str,
    column_specs: List[Dict[str, Any]],
) -> List[Partition]:
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
    df_work = (
        df.copy() if any(spec["kind"] == ColumnKind.CONTINUOUS for spec in column_specs) else df
    )

    grouping_columns = []
    influenced_counts = {}

    for spec in column_specs:
        col = spec["name"]

        if spec["kind"] == ColumnKind.CATEGORICAL:
            grouping_columns.append(col)
            influenced_counts[col] = df.groupby(privacy_unit)[col].nunique(dropna=True)

        elif spec["kind"] == ColumnKind.CONTINUOUS:
            bins = pd.to_datetime(spec["bins"]) if spec.get("is_datetime") else sorted(spec["bins"])
            binned_col = f"{col}__bin"
            df_work[binned_col] = pd.cut(df_work[col], bins=bins, right=False)
            grouping_columns.append(binned_col)
            influenced_counts[col] = df_work.groupby(privacy_unit)[binned_col].nunique(dropna=True)

        else:
            raise ValueError(f"Unknown column kind {spec['kind']}")

    partitions_meta: List[Partition] = []
    for group_key, group_df in df_work.groupby(grouping_columns, dropna=True, observed=True):
        per_privacy_unit_contrib = group_df.groupby(privacy_unit).size()
        max_contrib = max(
            int(influenced_counts[spec["name"]].loc[per_privacy_unit_contrib.index].max())
            for spec in column_specs
        )

        if len(column_specs) == 1:
            partitions_meta.append(
                SingleColumnPartition(
                    predicate=make_predicate(column_specs[0], group_key[0]),
                    max_length=int(group_df.shape[0]),
                    max_groups_per_unit=int(per_privacy_unit_contrib.max()),
                    max_contributions=max_contrib,
                )
            )
        else:
            partitions_meta.append(
                MultiColumnPartition(
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


def get_column_level_contribution(
    partitions_meta: Union[List[SingleColumnPartition], List[MultiColumnPartition]],
) -> Tuple[int, int, int]:
    """Compute maximum contribution over all partition of column."""
    max_length = max(p.max_length for p in partitions_meta)
    max_groups_per_unit = max(p.max_groups_per_unit for p in partitions_meta)
    max_contributions = max(p.max_contributions for p in partitions_meta)

    return max_length, max_groups_per_unit, max_contributions


def make_column_groups(
    df: pd.DataFrame,
    column_groups: List[List[str]],
    fine_contributions_level: Dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
    continuous_partitions: Dict[str, List[Any]],
    privacy_unit: str,
) -> List[ColumnGroupMetadata]:
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

    fine_contributions_level : dict[str, ContributionLevel]
        Mapping specifying contribution bound levels for specific columns.
        Values must be either ``"column"`` or ``"partition"``.

    default_contributions_level : ContributionLevel
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
        if default_contributions_level == ContributionLevel.COLUMN:
            max_length, max_groups_per_unit, max_contributions = get_column_level_contribution(
                partitions_meta
            )
            group_meta = ColumnGroupMetadata(
                columns=col_group,
                public_keys=full_partition_to_key_multi(partitions_meta),
                max_num_partitions=len(partitions_meta),
                max_length=max_length,
                max_groups_per_unit=max_groups_per_unit,
                max_contributions=max_contributions,
            )
        else:  # ContributionLevel.PARTITION
            group_meta = ColumnGroupMetadata(
                columns=col_group,
                partitions=partitions_meta,
                max_num_partitions=len(partitions_meta),
            )

        column_groups_metadata.append(group_meta)

    return column_groups_metadata


def prepare_metadata_inputs(
    default_contributions_level: str,
    fine_contributions_level: Optional[Dict[str, str]],
    continuous_partitions: Optional[Dict[str, List[Any]]],
    column_groups: Optional[List[List[str]]],
) -> Tuple[ContributionLevel, Dict[str, ContributionLevel], Dict[str, List[Any]], List[List[str]]]:
    """
    Normalize optional metadata configuration inputs.

    This helper ensures that optional parameters are initialized with
    appropriate defaults and applies implicit rules required by the
    metadata generation process.

    In particular:
    - Missing dictionaries/lists are replaced with empty structures.
    - Columns with numeric partitions are automatically treated at
      partition-level contribution granularity.

    Parameters
    ----------
    default_contributions_level : str

    fine_contributions_level : dict[str, str] or None
        Optional mapping specifying per-column contribution levels.
        Values must be one of {"table", "column", "partition"}.

    continuous_partitions : dict[str, list[Any]] or None
        Mapping of numeric column names to bin boundaries used
        to generate partitions.

    column_groups : list[list[str]] or None
        List of column groups used to create joint partitions.

    Returns
    -------
    tuple
        A tuple containing normalized versions of:

        - default_level : ContributionLevel
        - fine_level : dict[str, ContributionLevel]
        - continuous_partitions : dict[str, list[Any]]
        - column_groups : list[list[str]]
    """
    default_level = ContributionLevel.from_str(default_contributions_level)

    if continuous_partitions is None:
        continuous_partitions = {}

    if column_groups is None:
        column_groups = []

    if fine_contributions_level is None:
        fine_level = {}
    else:
        fine_level = {k: ContributionLevel.from_str(v) for k, v in fine_contributions_level.items()}

    for col in continuous_partitions:
        fine_level[col] = ContributionLevel.PARTITION

    return default_level, fine_level, continuous_partitions, column_groups


def attach_partitions_to_column(
    df: pd.DataFrame,
    column_meta: ColumnMetadata,
    column_name: str,
    privacy_unit: str,
    continuous_partitions: Dict[str, List[Any]],
    col_contrib_level: ContributionLevel,
) -> None:
    """
    Compute and attach partition metadata for a column.

    Depending on the contribution level, partitions may be stored either
    as full partition objects (partition-level contributions) or as a
    simplified list of public partition keys (column-level contributions).

    Categorical columns are partitioned by unique values, while numeric
    columns are discretized using provided bin boundaries.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset.

    column_meta : ColumnMetadata
        Metadata object that will be updated with partition information.

    column_name : str
        Name of the column being partitioned.

    privacy_unit : str
        Column representing the privacy unit.

    continuous_partitions : dict[str, list[Any]]
        Mapping of numeric column names to bin boundaries.

    col_contrib_level : ContributionLevel
        Contribution granularity applied to the column.

    Returns
    -------
    None
        The function modifies ``column_meta`` in place.
    """
    series = df[column_name]

    if is_categorical(series):

        partitions_meta = make_categorical_partitions(df, privacy_unit, column_name)
        column_meta.max_num_partitions = len(partitions_meta)

        if col_contrib_level == ContributionLevel.COLUMN:
            max_length, max_groups_per_unit, max_contributions = get_column_level_contribution(
                partitions_meta
            )
            column_meta.max_length = max_length
            column_meta.max_groups_per_unit = max_groups_per_unit
            column_meta.max_contributions = max_contributions
            column_meta.public_keys = full_partition_to_key_single(partitions_meta)
        else:
            column_meta.partitions = partitions_meta

    elif column_name in continuous_partitions:

        bounds = sorted(continuous_partitions[column_name])
        partitions_meta = make_numeric_partitions(df, privacy_unit, column_name, bounds)

        column_meta.partitions = partitions_meta
        column_meta.max_num_partitions = len(partitions_meta)


def build_column_metadata(
    df: pd.DataFrame,
    column_name: str,
    privacy_unit: str,
    continuous_partitions: Dict[str, List[Any]],
    fine_contributions_level: Dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
    with_dependencies: bool,
) -> ColumnMetadata:
    """
    Construct metadata for a single column.

    This function infers column properties and computes metadata fields
    required by CSVW-SAFE, including datatype inference, nullability,
    dependencies, fixed-per-entity attributes, and optional contribution
    partitions.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset.

    column_name : str
        Name of the column being processed.

    privacy_unit : str
        Name of the column representing the privacy unit.

    continuous_partitions : dict[str, list[Any]]
        Mapping of numeric column names to partition bin boundaries.

    fine_contributions_level : dict[str, str]
        Mapping specifying per-column contribution levels.

    default_contributions_level : str
        Default contribution level applied when a column is not explicitly
        listed in ``fine_contributions_level``.

    with_dependencies: bool

    Returns
    -------
    ColumnMetadata
        Metadata object describing the column according to the CSVW-SAFE
        specification.
    """
    # Column by itself (mainly CSVW)
    series = df[column_name]
    datatype = infer_xmlschema_datatype(series)
    column_meta = ColumnMetadata(
        name=column_name,
        datatype=datatype,
        required=series.isna().sum() == 0,
        privacy_id=(column_name == privacy_unit),
        nullable_proportion=np.ceil(series.isna().mean() * 1000) / 1000,
    )

    if datatype != DataTypes.STRING:
        minimum, maximum = get_continuous_bounds(series)
        column_meta.minimum = minimum
        column_meta.maximum = maximum

    # Privacy unit contributions (DP)
    col_contrib_level = get_effective_contrib_level(
        column_name, fine_contributions_level, default_contributions_level
    )
    if col_contrib_level != ContributionLevel.TABLE:
        attach_partitions_to_column(
            df,
            column_meta,
            column_name,
            privacy_unit,
            continuous_partitions,
            col_contrib_level,
        )

    # Dependencies between columns
    if with_dependencies:
        deps = identify_dependency(df, column_name)
        if deps:
            column_meta.dependencies = deps
    return column_meta


def make_metadata_from_data(
    df: pd.DataFrame,
    with_dependencies: bool = True,
    privacy_unit: str = "",
    max_contributions: int = 0,  # TODO: compute
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
    with_dependencies: bool
        Boolean if add dependencies between columns
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
    default_level, fine_level, continuous_partitions, column_groups = prepare_metadata_inputs(
        default_contributions_level,
        fine_contributions_level,
        continuous_partitions,
        column_groups,
    )
    if privacy_unit is None and (
        default_level != ContributionLevel.TABLE
        or any(level != ContributionLevel.TABLE for level in fine_level.values())
    ):
        raise ValueError(f"Privacy unit is None, only '{ContributionLevel.TABLE}' possible.")
    if privacy_unit not in df.columns:
        raise ValueError(f"Privacy unit column '{privacy_unit}' not found.")

    # Column
    columns_meta = [
        build_column_metadata(
            df,
            column_name,
            privacy_unit,
            continuous_partitions,
            fine_level,
            default_level,
            with_dependencies,
        )
        for column_name in df.columns
    ]

    groups_meta = None
    if column_groups:
        groups_meta = make_column_groups(
            df, column_groups, fine_level, default_level, continuous_partitions, privacy_unit
        )

    table_metadata = TableMetadata(
        privacy_unit=privacy_unit,
        max_contributions=max_contributions,
        max_length=len(df),
        public_length=len(df),
        columns=columns_meta,
        column_groups=groups_meta,
    )

    return sanitize(table_metadata.to_dict())


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

    --output : str, optional
        Output JSON file where the generated metadata will be written.
        Default is "metadata.json".

    --privacy-unit : str
        Name of the column representing the privacy unit (e.g., patient_id).

    --with_dependencies: bool. Default is True.

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

    parser.add_argument("--output", default="metadata.json", help="Output metadata JSON file")

    parser.add_argument(
        "--with_dependencies",
        default=True,
        help="Add dependencies between columns",
    )

    parser.add_argument(
        "--privacy_unit",
        help="Column defining the privacy unit (e.g., patient_id)",
    )

    parser.add_argument(
        "--max_contributions",
        help="Declared bounds.maxContributions (l_infinity)",
    )

    parser.add_argument(
        "--continuous_partitions",
        type=str,
        default=None,
        help="JSON string of bounds per continuous column",
    )

    parser.add_argument(
        "--column_groups", type=str, default=None, help="JSON string of column groups"
    )

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

    continuous_partitions = (
        json.loads(args.continuous_partitions) if args.continuous_partitions else {}
    )
    column_groups = json.loads(args.column_groups) if args.column_groups else []

    metadata = make_metadata_from_data(
        df=df,
        with_dependencies=args.with_dependencies,
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
