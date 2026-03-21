from __future__ import annotations

from datetime import UTC, datetime

import pytest

from illdashboard.models import Measurement
from illdashboard.services.markers import (
    MOSTLY_OUT_OF_RANGE_TAG,
    MULTIPLE_MEASUREMENTS_TAG,
    NO_RANGE_TAG,
    ONLY_IN_RANGE_TAG,
    ONLY_OUT_OF_RANGE_TAG,
    SOME_OUT_OF_RANGE_TAG,
    combine_marker_tags,
    derived_range_tags,
    marker_group_tag,
)


def _measurement(
    *,
    value: float | None = None,
    reference_low: float | None = None,
    reference_high: float | None = None,
    qualitative_bool: bool | None = None,
    unit_conversion_missing: bool = False,
) -> Measurement:
    measurement = Measurement(
        lab_file_id=1,
        measurement_type_id=1,
        raw_marker_name="CRP",
        normalized_marker_key="crp",
        canonical_value=value,
        canonical_reference_low=reference_low,
        canonical_reference_high=reference_high,
        qualitative_bool=qualitative_bool,
        measured_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    if unit_conversion_missing:
        measurement.unit_conversion_missing = True
    return measurement


@pytest.mark.parametrize(
    ("measurements", "expected_tags"),
    [
        (
            [_measurement(value=4.0, reference_low=0.0, reference_high=5.0)],
            [ONLY_IN_RANGE_TAG],
        ),
        (
            [_measurement(value=12.0, reference_low=0.0, reference_high=5.0)],
            [ONLY_OUT_OF_RANGE_TAG, MOSTLY_OUT_OF_RANGE_TAG, SOME_OUT_OF_RANGE_TAG],
        ),
        (
            [
                _measurement(value=12.0, reference_low=0.0, reference_high=5.0),
                _measurement(value=4.0, reference_low=0.0, reference_high=5.0),
            ],
            [SOME_OUT_OF_RANGE_TAG],
        ),
        (
            [
                _measurement(value=12.0, reference_low=0.0, reference_high=5.0),
                _measurement(value=4.0, reference_low=0.0, reference_high=5.0),
                _measurement(value=14.0, reference_low=0.0, reference_high=5.0),
            ],
            [MOSTLY_OUT_OF_RANGE_TAG, SOME_OUT_OF_RANGE_TAG],
        ),
        (
            [_measurement(value=12.0), _measurement(unit_conversion_missing=True)],
            [NO_RANGE_TAG],
        ),
    ],
)
def test_derived_range_tags_cover_requested_numeric_states(
    measurements: list[Measurement],
    expected_tags: list[str],
):
    assert derived_range_tags(measurements) == expected_tags


@pytest.mark.parametrize(
    ("qualitative_bool", "expected_tag"),
    [
        (False, ONLY_IN_RANGE_TAG),
        (True, ONLY_OUT_OF_RANGE_TAG),
    ],
)
def test_derived_range_tags_use_boolean_projection(
    qualitative_bool: bool,
    expected_tag: str,
):
    tags = derived_range_tags([_measurement(qualitative_bool=qualitative_bool)])
    assert expected_tag in tags
    if qualitative_bool:
        assert MOSTLY_OUT_OF_RANGE_TAG in tags
        assert SOME_OUT_OF_RANGE_TAG in tags


def test_derived_range_tags_reuse_marker_history_reference_for_missing_ranges():
    measurements = [
        _measurement(value=1826.0),
        _measurement(value=1712.0, reference_high=150.0),
    ]

    assert derived_range_tags(measurements) == [ONLY_OUT_OF_RANGE_TAG, MOSTLY_OUT_OF_RANGE_TAG, SOME_OUT_OF_RANGE_TAG]


def test_derived_range_tags_some_out_of_range_is_superset_of_other_out_of_range_buckets():
    all_out_of_range = derived_range_tags([
        _measurement(value=12.0, reference_low=0.0, reference_high=5.0),
    ])
    mostly_out_of_range = derived_range_tags([
        _measurement(value=12.0, reference_low=0.0, reference_high=5.0),
        _measurement(value=14.0, reference_low=0.0, reference_high=5.0),
        _measurement(value=4.0, reference_low=0.0, reference_high=5.0),
    ])

    assert SOME_OUT_OF_RANGE_TAG in all_out_of_range
    assert SOME_OUT_OF_RANGE_TAG in mostly_out_of_range
    assert MOSTLY_OUT_OF_RANGE_TAG in all_out_of_range


def test_combine_marker_tags_keeps_norange_when_history_still_has_unclassifiable_rows():
    measurements = [
        _measurement(value=4.0, reference_low=0.0, reference_high=5.0),
        _measurement(unit_conversion_missing=True),
    ]

    assert combine_marker_tags(["custom"], "Inflammation & Infection", measurements) == [
        "custom",
        marker_group_tag("Inflammation & Infection"),
        MULTIPLE_MEASUREMENTS_TAG,
        ONLY_IN_RANGE_TAG,
        NO_RANGE_TAG,
    ]
