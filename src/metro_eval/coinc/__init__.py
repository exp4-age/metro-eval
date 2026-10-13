"""Coincidence analysis package.

The package is organized around a small set of responsibilities:
- file reading and dataset discovery
- validation of coincidence keys and processing parameters
- pure processing routines
- calibration handling
- GUI widgets and application windows
"""

from metro_eval.coinc.core.validation import CoincidenceKey, parse_coincidence_key, validate_overlap_params
from metro_eval.coinc.core.workflow import CoincidenceWorkflow

__all__ = [
    "CoincidenceKey",
    "CoincidenceWorkflow",
    "parse_coincidence_key",
    "validate_overlap_params",
]
