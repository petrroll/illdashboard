"""Public GitHub Copilot integration helpers."""

from illdashboard.copilot.client import SessionEventType, shutdown_client
from illdashboard.copilot.explanations import explain_marker_history, explain_markers
from illdashboard.copilot.extraction import ocr_extract
from illdashboard.copilot.normalization import (
    AnomalousRescalingRequest,
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
    review_anomalous_rescaling,
)

__all__ = [
    "AnomalousRescalingRequest",
    "MarkerObservation",
    "MarkerUnitGroup",
    "QualitativeNormalizationRequest",
    "SessionEventType",
    "UnitConversionRequest",
    "choose_canonical_units",
    "classify_marker_groups",
    "explain_marker_history",
    "explain_markers",
    "infer_rescaling_factors",
    "normalize_marker_names",
    "normalize_qualitative_values",
    "normalize_source_name",
    "ocr_extract",
    "review_anomalous_rescaling",
    "shutdown_client",
]
