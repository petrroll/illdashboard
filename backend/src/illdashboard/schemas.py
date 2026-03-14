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


class ExplainResponse(BaseModel):
    explanation: str
