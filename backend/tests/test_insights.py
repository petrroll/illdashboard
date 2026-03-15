from datetime import datetime, timezone

from illdashboard.models import Measurement
from illdashboard.services.insights import fallback_marker_explanation


def test_fallback_marker_explanation_explains_out_of_range_without_generic_caution():
    measurement = Measurement(
        id=1,
        lab_file_id=1,
        measurement_type_id=1,
        value=15.0,
        unit="mg/L",
        reference_low=0.0,
        reference_high=5.0,
        measured_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
    )

    explanation = fallback_marker_explanation("CRP", [measurement])

    assert "above the reported range" in explanation
    assert "clinician" not in explanation.casefold()
    assert "not a diagnosis" not in explanation.casefold()


def test_fallback_marker_explanation_does_not_add_single_value_trend_filler():
    measurement = Measurement(
        id=1,
        lab_file_id=1,
        measurement_type_id=1,
        value=3.2,
        unit="mmol/L",
        reference_low=3.5,
        reference_high=5.1,
        measured_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
    )

    explanation = fallback_marker_explanation("Potassium", [measurement])

    assert "Compared with the previous result" not in explanation
    assert "lack of a trend" not in explanation.casefold()