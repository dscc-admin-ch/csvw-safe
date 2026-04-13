"""Utility files."""

import math
from enum import IntEnum
from typing import Any

import numpy as np


def sanitize(obj: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively convert objects into JSON/CSVW-SAFE serializable types.

    - NumPy scalars → Python scalars
    - NaN or Inf → None
    - Other types remain unchanged
    """
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, np.generic):
        obj = obj.item()  # convert NumPy scalar to native Python

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError("Value in Nan or infinite")
        return obj  # keep as float

    return obj  # leave everything else unchanged


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
