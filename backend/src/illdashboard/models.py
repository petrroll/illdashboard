import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text
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


class Measurement(Base):
    """A single lab value extracted from a file."""

    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lab_file_id = Column(Integer, ForeignKey("lab_files.id"), nullable=False)
    marker_name = Column(String, nullable=False)  # e.g. "Hemoglobin"
    value = Column(Float, nullable=False)
    unit = Column(String, nullable=True)  # e.g. "g/dL"
    reference_low = Column(Float, nullable=True)
    reference_high = Column(Float, nullable=True)
    measured_at = Column(DateTime, nullable=True)  # date/time of measurement

    lab_file = relationship("LabFile", back_populates="measurements")
