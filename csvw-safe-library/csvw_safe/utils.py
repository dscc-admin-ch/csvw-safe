"""Utility files."""

import math
from typing import Any, List

import numpy as np


def sanitize(obj: Any) -> Any:
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
            return None
        return obj  # keep as float

    return obj  # leave everything else unchanged


def error(msg: str, errors: List[str]) -> None:
    """Append an error message."""
    errors.append(msg)
