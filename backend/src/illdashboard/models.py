import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class LabFile(Base):
    """An uploaded lab file (PDF or image)."""

    __tablename__ = "lab_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String, nullable=False)
    filepath = Column(String, nullable=False)  # relative path under uploads/
    mime_type = Column(String, nullable=False)
    uploaded_at = Column(DateTime, default=datetime.datetime.utcnow)
    ocr_raw = Column(Text, nullable=True)  # raw OCR text
    lab_date = Column(DateTime, nullable=True)  # date of the lab report

    measurements = relationship("Measurement", back_populates="lab_file", cascade="all, delete-orphan")
    tags = relationship("LabFileTag", back_populates="lab_file", cascade="all, delete-orphan")


class MeasurementType(Base):
    """Canonical definition for a biomarker / measurement type."""

    __tablename__ = "measurement_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True, index=True)
    group_name = Column(String, nullable=False)

    measurements = relationship("Measurement", back_populates="measurement_type")
    tags = relationship("MarkerTag", back_populates="measurement_type", cascade="all, delete-orphan")
    insight = relationship(
        "BiomarkerInsight",
        back_populates="measurement_type",
        cascade="all, delete-orphan",
        uselist=False,
    )


class Measurement(Base):
    """A single lab value extracted from a file."""

    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lab_file_id = Column(Integer, ForeignKey("lab_files.id"), nullable=False)
    measurement_type_id = Column(Integer, ForeignKey("measurement_types.id"), nullable=False, index=True)
    value = Column(Float, nullable=False)
    unit = Column(String, nullable=True)  # e.g. "g/dL"
    reference_low = Column(Float, nullable=True)
    reference_high = Column(Float, nullable=True)
    measured_at = Column(DateTime, nullable=True)  # date/time of measurement
    page_number = Column(Integer, nullable=True)  # 1-indexed page in source file

    lab_file = relationship("LabFile", back_populates="measurements")
    measurement_type = relationship("MeasurementType", back_populates="measurements")

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    lab_file_id = Column(Integer, ForeignKey("lab_files.id", ondelete="CASCADE"), nullable=False)
    tag = Column(String, nullable=False)

    lab_file = relationship("LabFile", back_populates="tags")


class MarkerTag(Base):
    """A tag attached to a marker type (e.g. group, single/multiple)."""

    __tablename__ = "marker_tags"
    __table_args__ = (UniqueConstraint("measurement_type_id", "tag"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    measurement_type_id = Column(Integer, ForeignKey("measurement_types.id", ondelete="CASCADE"), nullable=False, index=True)
    tag = Column(String, nullable=False)

    measurement_type = relationship("MeasurementType", back_populates="tags")

    @property
    def marker_name(self) -> str:
        return self.measurement_type.name


class BiomarkerInsight(Base):
    """Cached AI summary for a biomarker and its latest trend state."""

    __tablename__ = "biomarker_insights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    measurement_type_id = Column(
        Integer,
        ForeignKey("measurement_types.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    measurement_signature = Column(String, nullable=False)
    summary_markdown = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    measurement_type = relationship("MeasurementType", back_populates="insight")

    @property
    def marker_name(self) -> str:
        return self.measurement_type.name
