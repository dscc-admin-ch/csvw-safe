"""
Create an OpenDP Context from CSVW-SAFE metadata and a dataset.

This module:
- Converts CSVW-SAFE metadata into OpenDP margins
- Builds an OpenDP Context using a provided dataset
- Supports epsilon-based (Laplace) and rho-based (Gaussian) DP
- Exposes both a Python API and CLI

The resulting context can be used for differentially private queries.
"""

import argparse
import json
from typing import Any

import opendp.prelude as dp
import polars as pl

from csvw_safe.constants import MAX_CONTRIB, PRIVACY_UNIT
from csvw_safe.csvw_to_opendp_margins import csvw_to_opendp_margins


def get_privacy_loss(
    epsilon: float | None = None,
    rho: float | None = None,
    delta: float | None = None,
) -> Any:
    """
    Create an opendp privacy loss object.

    Parameters
    ----------
    epsilon : float, optional
        Privacy budget epsilon (for Laplace DP).
    rho : float, optional
        Privacy budget rho (for Gaussian / zCDP).
    delta : float, optional
        Privacy budget delta (if using approximate DP).

    Returns
    -------
    privacy_loss
        opendp privacy loss object

    Raises
    ------
    ValueError
        If neither epsilon nor rho is provided.
    """
    if epsilon is None and rho is None:
        raise ValueError("Either epsilon or rho must be provided")

    if epsilon is not None and rho is not None:
        raise ValueError("Specify only one of epsilon or rho")

    if epsilon is not None:
        return dp.loss_of(epsilon=epsilon, delta=delta)

    return dp.loss_of(rho=rho, delta=delta)


def get_privacy_unit(csvw_meta: dict[str, Any], distance: str) -> Any:
    """
    Construct an OpenDP privacy unit from CSVW-SAFE metadata.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-SAFE metadata dictionary.

    Returns
    -------
    privacy_unit
        OpenDP privacy unit descriptor.
    """
    if MAX_CONTRIB not in csvw_meta:
        raise ValueError("Missing max_contributions in metadata")

    max_contrib = csvw_meta[MAX_CONTRIB]
    identifier = csvw_meta.get(PRIVACY_UNIT)

    kwargs: dict[str, Any] = {}

    # Map distance type → correct argument
    if distance == "contributions":
        kwargs["contributions"] = max_contrib
    elif distance == "changes":
        kwargs["changes"] = max_contrib
    # elif distance == "absolute":
    # kwargs["absolute"] = max_contrib
    # elif distance == "l1":
    # kwargs["l1"] = float(max_contrib)
    # elif distance == "l2":
    # kwargs["l2"] = float(max_contrib)
    else:
        raise ValueError(f"Unsupported distance type: {distance}")

    if identifier is not None:
        kwargs["identifier"] = pl.col(identifier)

    return dp.unit_of(**kwargs)


def csvw_to_opendp_context(
    csvw_meta: dict[str, Any],
    data: pl.LazyFrame,
    epsilon: float | None = None,
    rho: float | None = None,
    delta: float | None = None,
    split_evenly_over: int | None = None,
    split_by_weights: list[float] | None = None,
    distance: str = "contributions",
) -> dp.Context:
    """
    Create an OpenDP Context from CSVW-SAFE metadata and a dataset.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-SAFE metadata dictionary.
        Must include `csvw-safe.dp.maxContributions`.
    data : pl.LazyFrame
        Input dataset (recommended as LazyFrame).
    epsilon : float, optional
        Privacy budget epsilon (for Laplace DP).
    rho : float, optional
        Privacy budget rho (for Gaussian / zCDP).
    delta : float, optional
        Privacy budget delta (if using approximate DP).
    split_evenly_over : int
        Number of queries to split privacy budget across.
    split_by_weights: list[float]
        List of privacy budget weight by query.
    distance: str, default='contributions'
        Distance metric for privacy unit.

    Returns
    -------
    Context
        OpenDP Context object ready for queries.

    Raises
    ------
    ValueError
        If required metadata (max_contributions) is missing.
        If neither epsilon nor rho is provided.
    """
    if split_evenly_over is not None and split_by_weights is not None:
        raise ValueError("Specify only one of split_evenly_over or split_by_weights")

    kwargs: dict[str, Any] = {
        "data": data,
        "privacy_unit": get_privacy_unit(csvw_meta, distance),
        "privacy_loss": get_privacy_loss(epsilon, rho, delta),
        "margins": csvw_to_opendp_margins(csvw_meta),
    }
    if split_by_weights is not None:
        kwargs["split_by_weights"] = split_by_weights
    else:
        kwargs["split_evenly_over"] = split_evenly_over

    return dp.Context.compositor(**kwargs)


def main() -> None:
    """
    CLI to build an OpenDP Context from CSVW-SAFE metadata and a CSV file.

    Command-line arguments
    ----------------------
    --metadata : str (required)
        Path to CSVW-SAFE JSON metadata file.
    --data : str (required)
        Path to CSV file.
    --epsilon : float, optional
        Privacy budget epsilon (Laplace DP).
    --rho : float, optional
        Privacy budget rho (Gaussian DP / zCDP).
    --delta : float, optional
        Privacy budget delta (for approximate DP).
    --split_evenly_over : int (default=1)
        Number of queries to split privacy budget across.
    """
    parser = argparse.ArgumentParser(
        description="Create an OpenDP Context from CSVW-SAFE metadata and a dataset."
    )
    parser.add_argument("--metadata", required=True, help="CSVW-SAFE metadata JSON file")
    parser.add_argument("--data", required=True, help="CSV file")
    parser.add_argument("--epsilon", type=float, default=None, help="Privacy budget epsilon")
    parser.add_argument("--rho", type=float, default=None, help="Privacy budget rho")
    parser.add_argument("--delta", type=float, default=None, help="Privacy budget delta")
    parser.add_argument("--split_evenly_over", type=int, default=1)
    parser.add_argument(
        "--split_by_weights",
        type=float,
        nargs="+",
        help="Split privacy budget by weights (e.g. 1 2 3)",
    )
    parser.add_argument(
        "--distance",
        choices=["contributions", "changes"],  # "absolute", "l1", "l2"
        default="contributions",
        help="Distance metric for privacy unit",
    )

    args = parser.parse_args()

    # Load metadata
    with open(args.metadata, encoding="utf-8") as f:
        csvw_meta = json.load(f)

    # Load data as LazyFrame (recommended by OpenDP)
    data = pl.scan_csv(args.data, ignore_errors=True)

    # Build context
    context = csvw_to_opendp_context(
        csvw_meta=csvw_meta,
        data=data,
        epsilon=args.epsilon,
        rho=args.rho,
        delta=args.delta,
        split_evenly_over=args.split_evenly_over,
        split_by_weights=args.split_by_weights,
    )

    print("OpenDP Context successfully created.")
    print(context)


if __name__ == "__main__":
    main()
