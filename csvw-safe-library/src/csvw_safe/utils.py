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
    TABLE_WITH_KEYS = 1
    COLUMN = 2
    PARTITION = 3

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
        if value == "table_with_keys":
            return cls.TABLE_WITH_KEYS
        if value == "column":
            return cls.COLUMN
        if value == "partition":
            return cls.PARTITION
        raise ValueError(f"Invalid contribution level: {value}")


def get_effective_contrib_level(
    column_name: str,
    fine_contributions_level: dict[str, ContributionLevel],
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


def get_group_contribution_level(
    col_group: list[str],
    fine_contributions_level: dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
) -> ContributionLevel:
    """Determine the effective contribution level for a column group."""
    levels = [
        get_effective_contrib_level(col, fine_contributions_level, default_contributions_level)
        for col in col_group
    ]

    if any(level == ContributionLevel.TABLE for level in levels):
        raise ValueError(
            f"Invalid contribution level in ColumnGroup {col_group}: contains TABLE-level column."
        )

    # TABLE < TABLE_WITH_KEYS < COLUMN < PARTITION
    return min(levels)
