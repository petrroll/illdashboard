from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

UPLOADED_FILE_STATUS = "uploaded"
READY_FILE_STATUS = "ready"
DEFAULT_GROUP_NAME = "Other"


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class LabFile(Base):
    __tablename__ = "lab_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    filepath: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    mime_type: Mapped[str] = mapped_column(String, nullable=False)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String, nullable=False, default=UPLOADED_FILE_STATUS, index=True)
    measurement_status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    normalization_status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    text_status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    summary_status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    publish_status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_name: Mapped[str | None] = mapped_column(String, nullable=True)
    ocr_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_text_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_text_english: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_summary_english: Mapped[str | None] = mapped_column(Text, nullable=True)
    lab_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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

    @property
    def is_ready(self) -> bool:
        return self.status == READY_FILE_STATUS


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
