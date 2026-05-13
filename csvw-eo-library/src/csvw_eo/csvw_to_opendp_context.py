"""
Create an OpenDP Context from CSVW-EO metadata and a dataset.

This module:
- Converts CSVW-EO metadata into OpenDP margins
- Builds an OpenDP Context using a provided dataset
- Supports epsilon-based (Laplace) and rho-based (Gaussian) DP
- Exposes both a Python API and CLI

The resulting context can be used for differentially private queries.
"""

from collections.abc import Sequence
from typing import Any, Union

import opendp.prelude as dp
import polars as pl
from opendp.extras.polars import Bound
from opendp.mod import Measure, Metric, enable_features

from csvw_eo.constants import MAX_CONTRIB  # , PRIVACY_UNIT
from csvw_eo.csvw_to_opendp_margins import csvw_to_opendp_margins

enable_features("contrib")


def get_privacy_loss(
    epsilon: float | None = None,
    rho: float | None = None,
    delta: float | None = None,
) -> tuple[Measure, Any]:
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


def get_privacy_unit(
    csvw_meta: dict[str, Any], distance: str
) -> tuple[Metric, Union[float, Sequence[Bound]]]:
    """
    Construct an OpenDP privacy unit from CSVW-EO metadata.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-EO metadata dictionary.
    distance : str
        Type of privacy distance metric to use (e.g. "contributions", "changes").

    Returns
    -------
    privacy_unit
        OpenDP privacy unit descriptor.

    """
    if MAX_CONTRIB not in csvw_meta:
        raise ValueError("Missing max_contributions in metadata")

    max_contrib = csvw_meta[MAX_CONTRIB]

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

    # identifier = csvw_meta.get(PRIVACY_UNIT)
    # if identifier is not None:
    #     kwargs["identifier"] = pl.col(identifier)  # TODO: investigate more

    return dp.unit_of(**kwargs)


def csvw_to_opendp_context(  # noqa: PLR0913
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
    Create an OpenDP Context from CSVW-EO metadata and a dataset.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-EO metadata dictionary.
        Must include `csvw-eo.dp.maxContributions`.
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
