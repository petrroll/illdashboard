from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

UPLOADED_FILE_STATUS = "uploaded"
QUEUED_FILE_STATUS = "queued"
PROCESSING_FILE_STATUS = "processing"
COMPLETE_FILE_STATUS = "complete"
ERROR_FILE_STATUS = "error"
DEFAULT_GROUP_NAME = "Other"


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class SchemaMigration(Base):
    __tablename__ = "schema_migrations"

    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class LabFile(Base):
    __tablename__ = "lab_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    filepath: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    mime_type: Mapped[str] = mapped_column(String, nullable=False)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String, nullable=False, default=UPLOADED_FILE_STATUS, index=True)
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_candidate: Mapped[str | None] = mapped_column(String, nullable=True)
    source_candidate_key: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    source_name: Mapped[str | None] = mapped_column(String, nullable=True)
    ocr_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_text_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_text_english: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_summary_english: Mapped[str | None] = mapped_column(Text, nullable=True)
    lab_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user_lab_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user_lab_date_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    text_assembled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    summary_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    search_indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    measurements: Mapped[list[Measurement]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )
    tags: Mapped[list[LabFileTag]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )
    jobs: Mapped[list[Job]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )
    measurement_batches: Mapped[list[MeasurementBatch]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )
    text_batches: Mapped[list[TextBatch]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )

    @property
    def is_complete(self) -> bool:
        return self.status == COMPLETE_FILE_STATUS

    @property
    def effective_lab_date(self) -> datetime | None:
        return self.user_lab_date if self.user_lab_date_override else self.lab_date

    @property
    def user_edited_fields(self) -> list[str]:
        fields: list[str] = []
        if self.user_lab_date_override:
            fields.append("lab_date")
        return fields

    @property
    def has_user_edits(self) -> bool:
        return bool(self.user_edited_fields)


class MeasurementBatch(Base):
    __tablename__ = "measurement_batches"
    __table_args__ = (UniqueConstraint("file_id", "task_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("lab_files.id", ondelete="CASCADE"), nullable=False, index=True)
    task_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    start_page: Mapped[int] = mapped_column(Integer, nullable=False)
    stop_page: Mapped[int] = mapped_column(Integer, nullable=False)
    dpi: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    lab_file: Mapped[LabFile] = relationship(back_populates="measurement_batches")


class TextBatch(Base):
    __tablename__ = "text_batches"
    __table_args__ = (UniqueConstraint("file_id", "task_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("lab_files.id", ondelete="CASCADE"), nullable=False, index=True)
    task_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    start_page: Mapped[int] = mapped_column(Integer, nullable=False)
    stop_page: Mapped[int] = mapped_column(Integer, nullable=False)
    dpi: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    translated_text_english: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    lab_file: Mapped[LabFile] = relationship(back_populates="text_batches")


class MarkerGroup(Base):
    __tablename__ = "marker_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    display_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    measurement_types: Mapped[list[MeasurementType]] = relationship(back_populates="group")


class MeasurementType(Base):
    __tablename__ = "measurement_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    normalized_key: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    group_name: Mapped[str] = mapped_column(String, nullable=False, default=DEFAULT_GROUP_NAME)
    group_id: Mapped[int | None] = mapped_column(
        ForeignKey("marker_groups.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    canonical_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    group: Mapped[MarkerGroup | None] = relationship(back_populates="measurement_types")
    measurements: Mapped[list[Measurement]] = relationship(back_populates="measurement_type")
    tags: Mapped[list[MarkerTag]] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
    )
    aliases: Mapped[list[MeasurementAlias]] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
    )
    rescaling_rules: Mapped[list[RescalingRule]] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
    )
    qualitative_rules: Mapped[list[QualitativeRule]] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
    )
    insight: Mapped[BiomarkerInsight | None] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
        uselist=False,
    )


class Measurement(Base):
    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lab_file_id: Mapped[int] = mapped_column(ForeignKey("lab_files.id", ondelete="CASCADE"), nullable=False, index=True)
    measurement_type_id: Mapped[int | None] = mapped_column(
        ForeignKey("measurement_types.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    raw_marker_name: Mapped[str] = mapped_column(String, nullable=False)
    normalized_marker_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    original_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    original_qualitative_value: Mapped[str | None] = mapped_column(String, nullable=True)
    qualitative_bool: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    qualitative_value: Mapped[str | None] = mapped_column(String, nullable=True)
    original_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    normalized_original_unit: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    canonical_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    canonical_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    original_reference_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    original_reference_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    canonical_reference_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    canonical_reference_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    user_original_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    user_original_unit_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_canonical_unit: Mapped[str | None] = mapped_column(String, nullable=True)
    user_canonical_unit_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_canonical_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    user_canonical_value_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_qualitative_value: Mapped[str | None] = mapped_column(String, nullable=True)
    user_qualitative_value_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_qualitative_bool: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    user_qualitative_bool_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_canonical_reference_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    user_canonical_reference_low_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_canonical_reference_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    user_canonical_reference_high_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_measured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user_measured_at_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    user_edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    measured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    batch_key: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    normalization_status: Mapped[str] = mapped_column(String, nullable=False, default="pending", index=True)
    normalization_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    lab_file: Mapped[LabFile] = relationship(back_populates="measurements")
    measurement_type: Mapped[MeasurementType | None] = relationship(back_populates="measurements")

    @property
    def marker_name(self) -> str:
        if self.measurement_type is not None:
            return self.measurement_type.name
        return self.raw_marker_name

    @property
    def group_name(self) -> str:
        if self.measurement_type is not None and self.measurement_type.group_name:
            return self.measurement_type.group_name
        return DEFAULT_GROUP_NAME

    @property
    def effective_original_unit(self) -> str | None:
        return self.user_original_unit if self.user_original_unit_override else self.original_unit

    @property
    def effective_canonical_unit(self) -> str | None:
        if self.user_canonical_unit_override:
            return self.user_canonical_unit
        if self.canonical_unit is not None:
            return self.canonical_unit
        if self.measurement_type is not None:
            return self.measurement_type.canonical_unit
        return None

    @property
    def effective_canonical_value(self) -> float | None:
        return self.user_canonical_value if self.user_canonical_value_override else self.canonical_value

    @property
    def effective_qualitative_value(self) -> str | None:
        return self.user_qualitative_value if self.user_qualitative_value_override else self.qualitative_value

    @property
    def effective_qualitative_bool(self) -> bool | None:
        return self.user_qualitative_bool if self.user_qualitative_bool_override else self.qualitative_bool

    @property
    def effective_canonical_reference_low(self) -> float | None:
        if self.user_canonical_reference_low_override:
            return self.user_canonical_reference_low
        return self.canonical_reference_low

    @property
    def effective_canonical_reference_high(self) -> float | None:
        if self.user_canonical_reference_high_override:
            return self.user_canonical_reference_high
        return self.canonical_reference_high

    @property
    def effective_measured_at(self) -> datetime | None:
        return self.user_measured_at if self.user_measured_at_override else self.measured_at

    @property
    def user_edited_fields(self) -> list[str]:
        fields: list[str] = []
        if self.user_canonical_value_override:
            fields.append("canonical_value")
        if self.user_canonical_unit_override:
            fields.append("canonical_unit")
        if self.user_original_unit_override:
            fields.append("original_unit")
        if self.user_qualitative_value_override:
            fields.append("qualitative_value")
        if self.user_qualitative_bool_override:
            fields.append("qualitative_bool")
        if self.user_canonical_reference_low_override:
            fields.append("canonical_reference_low")
        if self.user_canonical_reference_high_override:
            fields.append("canonical_reference_high")
        if self.user_measured_at_override:
            fields.append("measured_at")
        return fields

    @property
    def has_user_edits(self) -> bool:
        return bool(self.user_edited_fields)


class LabFileTag(Base):
    __tablename__ = "lab_file_tags"
    __table_args__ = (UniqueConstraint("lab_file_id", "tag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lab_file_id: Mapped[int] = mapped_column(
        ForeignKey("lab_files.id", ondelete="CASCADE"),
        nullable=False,
    )
    tag: Mapped[str] = mapped_column(String, nullable=False)

    lab_file: Mapped[LabFile] = relationship(back_populates="tags")


class MarkerTag(Base):
    __tablename__ = "marker_tags"
    __table_args__ = (UniqueConstraint("measurement_type_id", "tag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    measurement_type_id: Mapped[int] = mapped_column(
        ForeignKey("measurement_types.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tag: Mapped[str] = mapped_column(String, nullable=False)

    measurement_type: Mapped[MeasurementType] = relationship(back_populates="tags")

    @property
    def marker_name(self) -> str:
        return self.measurement_type.name


class MeasurementAlias(Base):
    __tablename__ = "measurement_aliases"
    __table_args__ = (UniqueConstraint("normalized_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alias_name: Mapped[str] = mapped_column(String, nullable=False)
    normalized_key: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    measurement_type_id: Mapped[int] = mapped_column(
        ForeignKey("measurement_types.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    measurement_type: Mapped[MeasurementType] = relationship(back_populates="aliases")


class SourceAlias(Base):
    __tablename__ = "source_aliases"
    __table_args__ = (UniqueConstraint("normalized_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alias_name: Mapped[str] = mapped_column(String, nullable=False)
    normalized_key: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    canonical_name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (UniqueConstraint("task_type", "task_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int | None] = mapped_column(
        ForeignKey("lab_files.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    task_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    task_key: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending", index=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    resolved_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    rerun_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)
    lease_owner: Mapped[str | None] = mapped_column(String, nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    lab_file: Mapped[LabFile | None] = relationship(back_populates="jobs")


class RescalingRule(Base):
    __tablename__ = "rescaling_rules"
    __table_args__ = (UniqueConstraint("measurement_type_id", "normalized_original_unit", "normalized_canonical_unit"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    measurement_type_id: Mapped[int] = mapped_column(
        ForeignKey("measurement_types.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    original_unit: Mapped[str] = mapped_column(String, nullable=False)
    canonical_unit: Mapped[str] = mapped_column(String, nullable=False)
    scale_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    normalized_original_unit: Mapped[str] = mapped_column(String, nullable=False, index=True)
    normalized_canonical_unit: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    measurement_type: Mapped[MeasurementType] = relationship(back_populates="rescaling_rules")


class QualitativeRule(Base):
    __tablename__ = "qualitative_rules"
    __table_args__ = (UniqueConstraint("normalized_original_value"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    original_value: Mapped[str] = mapped_column(String, nullable=False)
    canonical_value: Mapped[str] = mapped_column(String, nullable=False)
    boolean_value: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    normalized_original_value: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    measurement_type_id: Mapped[int | None] = mapped_column(
        ForeignKey("measurement_types.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    measurement_type: Mapped[MeasurementType | None] = relationship(back_populates="qualitative_rules")


class BiomarkerInsight(Base):
    __tablename__ = "biomarker_insights"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    measurement_type_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("measurement_types.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    measurement_signature: Mapped[str] = mapped_column(String, nullable=False)
    summary_markdown: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    measurement_type: Mapped[MeasurementType] = relationship(back_populates="insight")

    @property
    def marker_name(self) -> str:
        return self.measurement_type.name
