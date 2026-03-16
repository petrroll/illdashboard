from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ── LabFile ──────────────────────────────────────────────────────────────────


class LabFileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    filename: str
    filepath: str
    mime_type: str
    uploaded_at: datetime
    ocr_raw: str | None = None
    ocr_text_raw: str | None = None
    ocr_text_english: str | None = None
    ocr_summary_english: str | None = None
    lab_date: datetime | None = None
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _flatten_tags(cls, data: Any):
        """Convert LabFileTag ORM objects to plain strings."""
        if isinstance(data, dict):
            return data
        if hasattr(data, "__table__"):
            raw = data.__dict__.get("tags", [])
            data = {c.key: getattr(data, c.key) for c in data.__table__.columns}
            data["tags"] = [t.tag for t in raw] if raw and hasattr(raw[0], "tag") else list(raw)
        return data


# ── Measurement ──────────────────────────────────────────────────────────────


class MeasurementOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    lab_file_id: int
    lab_file_filename: str | None = None
    lab_file_source_tag: str | None = None
    marker_name: str
    canonical_unit: str | None = None
    canonical_value: float | None = None
    original_value: float | None = None
    original_qualitative_value: str | None = None
    qualitative_bool: bool | None = None
    qualitative_value: str | None = None
    original_unit: str | None = None
    unit_conversion_missing: bool = False
    canonical_reference_low: float | None = None
    canonical_reference_high: float | None = None
    original_reference_low: float | None = None
    original_reference_high: float | None = None
    measured_at: datetime | None = None
    page_number: int | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_measurement_type(cls, data: Any):
        if isinstance(data, dict):
            data.setdefault("unit_conversion_missing", False)
            return data
        if hasattr(data, "measurement_type"):
            measurement_type = data.measurement_type
            lab_file = data.__dict__.get("lab_file")
            lab_file_tags = lab_file.__dict__.get("tags", []) if lab_file is not None else []
            source_tag = next(
                (
                    tag.tag
                    for tag in lab_file_tags
                    if hasattr(tag, "tag") and tag.tag.casefold().startswith("source:")
                ),
                None,
            )
            return {
                "id": data.id,
                "lab_file_id": data.lab_file_id,
                "lab_file_filename": getattr(lab_file, "filename", None),
                "lab_file_source_tag": source_tag,
                "marker_name": measurement_type.name,
                "canonical_unit": measurement_type.canonical_unit,
                "canonical_value": data.canonical_value,
                "original_value": data.original_value,
                "original_qualitative_value": data.original_qualitative_value,
                "qualitative_bool": data.qualitative_bool,
                "qualitative_value": data.qualitative_value,
                "original_unit": data.original_unit,
                "unit_conversion_missing": bool(getattr(data, "unit_conversion_missing", False)),
                "canonical_reference_low": data.canonical_reference_low,
                "canonical_reference_high": data.canonical_reference_high,
                "original_reference_low": data.original_reference_low,
                "original_reference_high": data.original_reference_high,
                "measured_at": data.measured_at,
                "page_number": data.page_number,
            }
        return data


class MarkerOverviewItem(BaseModel):
    marker_name: str
    group_name: str
    canonical_unit: str | None = None
    latest_measurement: MeasurementOut
    previous_measurement: MeasurementOut | None = None
    status: str
    range_position: float | None = None
    has_numeric_history: bool = False
    total_count: int = 1
    value_min: float | None = None
    value_max: float | None = None
    tags: list[str] = Field(default_factory=list)
    marker_tags: list[str] = Field(default_factory=list)
    file_tags: list[str] = Field(default_factory=list)


class MarkerOverviewGroup(BaseModel):
    group_name: str
    markers: list[MarkerOverviewItem]


class MarkerDetailResponse(BaseModel):
    marker_name: str
    group_name: str
    canonical_unit: str | None = None
    latest_measurement: MeasurementOut
    previous_measurement: MeasurementOut | None = None
    status: str
    range_position: float | None = None
    has_numeric_history: bool = False
    measurements: list[MeasurementOut]
    explanation: str | None = None
    explanation_cached: bool = False
    tags: list[str] = Field(default_factory=list)
    marker_tags: list[str] = Field(default_factory=list)
    file_tags: list[str] = Field(default_factory=list)


class MarkerInsightResponse(BaseModel):
    marker_name: str
    explanation: str
    explanation_cached: bool


class MeasurementCreate(BaseModel):
    marker_name: str
    value: float | None = None
    qualitative_value: str | None = None
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None
    measured_at: datetime | None = None


# ── AI / explanation ─────────────────────────────────────────────────────────


class ExplainRequest(BaseModel):
    marker_name: str
    value: float | None = None
    qualitative_value: str | None = None
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None


class MultiExplainRequest(BaseModel):
    """Ask the AI to explain a set of values together."""

    measurements: list[ExplainRequest]


# ── Batch OCR ────────────────────────────────────────────────────────────────


class BatchOcrRequest(BaseModel):
    file_ids: list[int]


class OcrJobProgressOut(BaseModel):
    file_id: int
    filename: str
    index: int
    total: int
    status: str
    error: str | None = None


class OcrJobStartResponse(BaseModel):
    job_id: str


class OcrJobStatusResponse(BaseModel):
    job_id: str
    status: str
    total: int
    completed_count: int
    error_count: int
    last_updated_at: float
    progress: list[OcrJobProgressOut] = Field(default_factory=list)


class ExplainResponse(BaseModel):
    explanation: str


class TagsUpdate(BaseModel):
    tags: list[str]


class SearchSnippet(BaseModel):
    source: str
    text: str


class SearchResultOut(BaseModel):
    file_id: int
    filename: str
    uploaded_at: datetime
    lab_date: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    marker_names: list[str] = Field(default_factory=list)
    snippets: list[SearchSnippet] = Field(default_factory=list)


class RescalingRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    original_unit: str
    canonical_unit: str
    scale_factor: float | None = None
    marker_name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_marker_name(cls, data: Any):
        if isinstance(data, dict):
            return data
        if hasattr(data, "__table__"):
            return {
                "id": data.id,
                "original_unit": data.original_unit,
                "canonical_unit": data.canonical_unit,
                "scale_factor": data.scale_factor,
                "marker_name": getattr(getattr(data, "measurement_type", None), "name", None),
            }
        return data
