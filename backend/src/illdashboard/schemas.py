from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class LabFileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    filename: str
    filepath: str
    mime_type: str
    page_count: int
    status: str
    measurement_status: str
    normalization_status: str
    text_status: str
    summary_status: str
    publish_status: str
    processing_error: str | None = None
    uploaded_at: datetime
    published_at: datetime | None = None
    ocr_raw: str | None = None
    ocr_text_raw: str | None = None
    ocr_text_english: str | None = None
    ocr_summary_english: str | None = None
    lab_date: datetime | None = None
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _flatten_tags(cls, data: Any):
        if isinstance(data, dict):
            return data
        if hasattr(data, "__table__"):
            raw_tags = data.__dict__.get("tags", [])
            data = {column.key: getattr(data, column.key) for column in data.__table__.columns}
            data["tags"] = [tag.tag for tag in raw_tags] if raw_tags and hasattr(raw_tags[0], "tag") else list(raw_tags)
        return data


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
        if hasattr(data, "lab_file"):
            measurement_type = getattr(data, "measurement_type", None)
            lab_file = getattr(data, "lab_file", None)
            lab_file_tags = lab_file.__dict__.get("tags", []) if lab_file is not None else []
            source_tag = next(
                (tag.tag for tag in lab_file_tags if hasattr(tag, "tag") and tag.tag.casefold().startswith("source:")),
                None,
            )
            return {
                "id": data.id,
                "lab_file_id": data.lab_file_id,
                "lab_file_filename": getattr(lab_file, "filename", None),
                "lab_file_source_tag": source_tag,
                "marker_name": getattr(data, "marker_name", None) or getattr(measurement_type, "name", None),
                "canonical_unit": data.canonical_unit or getattr(measurement_type, "canonical_unit", None),
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
    reference_low: float | None = None
    reference_high: float | None = None
    status: str
    range_position: float | None = None
    has_numeric_history: bool = False
    has_qualitative_trend: bool = False
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
    reference_low: float | None = None
    reference_high: float | None = None
    status: str
    range_position: float | None = None
    has_numeric_history: bool = False
    has_qualitative_trend: bool = False
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


class ExplainRequest(BaseModel):
    marker_name: str
    value: float | None = None
    qualitative_value: str | None = None
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None


class MultiExplainRequest(BaseModel):
    measurements: list[ExplainRequest]


class BatchOcrRequest(BaseModel):
    file_ids: list[int]


class QueueFilesResponse(BaseModel):
    queued_file_ids: list[int] = Field(default_factory=list)


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
