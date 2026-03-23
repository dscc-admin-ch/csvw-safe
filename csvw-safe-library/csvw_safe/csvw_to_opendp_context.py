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
from typing import Any, Dict, Optional

import opendp.prelude as dp
import polars as pl

from csvw_safe.constants import MAX_CONTRIB
from csvw_safe.csvw_to_opendp_margins import csvw_to_opendp_margins


def csvw_to_opendp_context(
    csvw_meta: Dict[str, Any],
    data: pl.LazyFrame,
    epsilon: Optional[float] = None,
    rho: Optional[float] = None,
    delta: Optional[float] = None,
    split_evenly_over: int = 1,
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
    split_evenly_over : int, default=1
        Number of queries to split privacy budget across.

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
    if MAX_CONTRIB not in csvw_meta:
        raise ValueError(f"Missing required field '{MAX_CONTRIB}'")

    max_contrib = csvw_meta[MAX_CONTRIB]

    if epsilon is None and rho is None:
        raise ValueError("Either epsilon or rho must be provided")

    # Build margins
    margins = csvw_to_opendp_margins(csvw_meta)

    # Privacy unit
    privacy_unit = dp.unit_of(contributions=max_contrib)

    # Laplace / standard DP
    if epsilon is not None:
        privacy_loss = dp.loss_of(epsilon=epsilon, delta=delta)
        context = dp.Context.compositor(
            data=data,
            privacy_unit=privacy_unit,
            privacy_loss=privacy_loss,
            split_evenly_over=split_evenly_over,
            margins=margins,
        )
        return context

    # Gaussian / zCDP
    # If delta is not provided, it will be ignored by dp.loss_of
    privacy_loss = dp.loss_of(rho=rho, delta=delta)
    context = dp.Context.compositor(
        data=data,
        privacy_unit=privacy_unit,
        privacy_loss=privacy_loss,
        split_evenly_over=split_evenly_over,
        margins=margins,
    )
    return context


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

    args = parser.parse_args()

    # Load metadata
    with open(args.metadata, "r", encoding="utf-8") as f:
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
    )

    print("OpenDP Context successfully created.")
    print(context)


if __name__ == "__main__":
    main()
