from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class LabFile(Base):
    """An uploaded lab file (PDF or image)."""

    __tablename__ = "lab_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    filepath: Mapped[str] = mapped_column(String, nullable=False)
    mime_type: Mapped[str] = mapped_column(String, nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    ocr_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    lab_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    measurements: Mapped[list[Measurement]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )
    tags: Mapped[list[LabFileTag]] = relationship(
        back_populates="lab_file",
        cascade="all, delete-orphan",
    )


class MeasurementType(Base):
    """Canonical definition for a biomarker / measurement type."""

    __tablename__ = "measurement_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    group_name: Mapped[str] = mapped_column(String, nullable=False)

    measurements: Mapped[list[Measurement]] = relationship(back_populates="measurement_type")
    tags: Mapped[list[MarkerTag]] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
    )
    insight: Mapped[BiomarkerInsight | None] = relationship(
        back_populates="measurement_type",
        cascade="all, delete-orphan",
        uselist=False,
    )


class Measurement(Base):
    """A single lab value extracted from a file."""

    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lab_file_id: Mapped[int] = mapped_column(ForeignKey("lab_files.id"), nullable=False)
    measurement_type_id: Mapped[int] = mapped_column(
        ForeignKey("measurement_types.id"),
        nullable=False,
        index=True,
    )
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    qualitative_value: Mapped[str | None] = mapped_column(String, nullable=True)
    unit: Mapped[str | None] = mapped_column(String, nullable=True)
    reference_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    reference_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    measured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)

    lab_file: Mapped[LabFile] = relationship(back_populates="measurements")
    measurement_type: Mapped[MeasurementType] = relationship(back_populates="measurements")

    @property
    def marker_name(self) -> str:
        return self.measurement_type.name

    @property
    def group_name(self) -> str:
        return self.measurement_type.group_name


class LabFileTag(Base):
    """A tag attached to a lab file (e.g. source like 'synlab')."""

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
    """A tag attached to a marker type (e.g. group, single/multiple)."""

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


class BiomarkerInsight(Base):
    """Cached AI summary for a biomarker and its latest trend state."""

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
