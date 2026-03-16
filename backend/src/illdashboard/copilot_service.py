"""Compatibility facade for Copilot helpers.

New code should import from the smaller modules under illdashboard.copilot.
"""

from illdashboard.copilot.client import SessionEventType, _ask, _ask_json, _get_client, shutdown_client
from illdashboard.copilot.explanations import explain_marker_history, explain_markers
from illdashboard.copilot.normalization import (
    MarkerObservation,
    MarkerUnitGroup,
    QualitativeNormalizationRequest,
    UnitConversionRequest,
    choose_canonical_units,
    classify_marker_groups,
    infer_rescaling_factors,
    normalize_marker_names,
    normalize_qualitative_values,
    normalize_source_name,
)
from illdashboard.copilot.ocr import ocr_extract

__all__ = [
    "MarkerObservation",
    "MarkerUnitGroup",
    "QualitativeNormalizationRequest",
    "SessionEventType",
    "UnitConversionRequest",
    "_ask",
    "_ask_json",
    "_get_client",
    "choose_canonical_units",
    "classify_marker_groups",
    "explain_marker_history",
    "explain_markers",
    "infer_rescaling_factors",
    "normalize_marker_names",
    "normalize_qualitative_values",
    "normalize_source_name",
    "ocr_extract",
    "shutdown_client",
]
