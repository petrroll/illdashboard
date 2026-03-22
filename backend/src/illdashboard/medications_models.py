from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from illdashboard.models import utc_now


class MedicationsBase(DeclarativeBase):
    pass


class Medication(MedicationsBase):
    __tablename__ = "medications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    episodes: Mapped[list[MedicationEpisode]] = relationship(
        back_populates="medication",
        cascade="all, delete-orphan",
        order_by="MedicationEpisode.position, MedicationEpisode.id",
    )


class MedicationEpisode(MedicationsBase):
    __tablename__ = "medication_episodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    medication_id: Mapped[int] = mapped_column(
        ForeignKey("medications.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    start_on: Mapped[str] = mapped_column(String, nullable=False)
    end_on: Mapped[str | None] = mapped_column(String, nullable=True)
    still_taking: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dose: Mapped[str] = mapped_column(String, nullable=False)
    frequency: Mapped[str] = mapped_column(String, nullable=False, default="daily")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    medication: Mapped[Medication] = relationship(back_populates="episodes")


class TimelineEvent(MedicationsBase):
    __tablename__ = "timeline_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    occurrences: Mapped[list[TimelineEventOccurrence]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
        order_by="TimelineEventOccurrence.position, TimelineEventOccurrence.id",
    )


class TimelineEventOccurrence(MedicationsBase):
    __tablename__ = "timeline_event_occurrences"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("timeline_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    start_on: Mapped[str] = mapped_column(String, nullable=False)
    end_on: Mapped[str | None] = mapped_column(String, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    event: Mapped[TimelineEvent] = relationship(back_populates="occurrences")
