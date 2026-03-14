from datetime import datetime

from pydantic import BaseModel

# ── LabFile ──────────────────────────────────────────────────────────────────


class LabFileOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    filename: str
    filepath: str
    mime_type: str
    uploaded_at: datetime
    ocr_raw: str | None = None
    lab_date: datetime | None = None


# ── Measurement ──────────────────────────────────────────────────────────────


class MeasurementOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    lab_file_id: int
    marker_name: str
    value: float
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None
    measured_at: datetime | None = None
    page_number: int | None = None


class MarkerOverviewItem(BaseModel):
    marker_name: str
    group_name: str
    latest_measurement: MeasurementOut
    previous_measurement: MeasurementOut | None = None
    status: str
    range_position: float | None = None
    total_count: int = 1
    value_min: float | None = None
    value_max: float | None = None


class MarkerOverviewGroup(BaseModel):
    group_name: str
    markers: list[MarkerOverviewItem]


class MarkerDetailResponse(BaseModel):
    marker_name: str
    group_name: str
    latest_measurement: MeasurementOut
    previous_measurement: MeasurementOut | None = None
    status: str
    range_position: float | None = None
    measurements: list[MeasurementOut]
    explanation: str | None = None
    explanation_cached: bool = False


class MarkerInsightResponse(BaseModel):
    marker_name: str
    explanation: str
    explanation_cached: bool


class MeasurementCreate(BaseModel):
    marker_name: str
    value: float
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None
    measured_at: datetime | None = None


# ── AI / explanation ─────────────────────────────────────────────────────────


class ExplainRequest(BaseModel):
    marker_name: str
    value: float
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None


class MultiExplainRequest(BaseModel):
    """Ask the AI to explain a set of values together."""

    measurements: list[ExplainRequest]


# ── Batch OCR ────────────────────────────────────────────────────────────────


class BatchOcrRequest(BaseModel):
    file_ids: list[int]


class ExplainResponse(BaseModel):
    explanation: str
