import uuid
from datetime import datetime, timedelta

from sqlalchemy import UUID, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from flight_blender.db import Base

RECOVERY_TYPE_CHOICES = [
    ("automatic", "Automatic"),
    ("manual", "Manual"),
]


def get_thirty_minutes_from_now():
    return datetime.utcnow() + timedelta(minutes=30)


class SurveillanceSession(Base):
    __tablename__ = "surveillance_session"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    valid_until = Column(DateTime, default=get_thirty_minutes_from_now)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    heartbeat_events = relationship("SurveillanceHeartbeatEvent", back_populates="session")
    track_events = relationship("SurveillanceTrackEvent", back_populates="session")

    def __str__(self):
        return str(self.id)


class SurveillanceSensor(Base):
    __tablename__ = "surveillance_sensor"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_type = Column(Integer, default=12)
    sensor_identifier = Column(String(256), unique=True, nullable=False)
    refresh_rate_seconds = Column(Float, default=1.0)
    is_active = Column(Boolean, default=True)
    horizontal_accuracy_m = Column(Float, default=5.0)
    vertical_accuracy_m = Column(Float, default=5.0)
    expected_latency_ms = Column(Integer, default=150)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __str__(self):
        return f"{self.sensor_type} - {self.sensor_identifier}"


class SurveillanceSensorHealth(Base):
    __tablename__ = "surveillance_sensor_health"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_sensor.id"), unique=True)
    status = Column(String(12), nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sensor = relationship("SurveillanceSensor")


class SurveillanceSensortHealthTracking(Base):
    __tablename__ = "surveillance_sensort_health_tracking"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_sensor.id"))
    status = Column(String(12), nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow)
    recovery_type = Column(String(12), nullable=True)

    sensor = relationship("SurveillanceSensor")


class SurveillanceSensorMaintenance(Base):
    __tablename__ = "surveillance_sensor_maintenance"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_sensor.id"), unique=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    planned_or_unplanned = Column(String(12), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sensor = relationship("SurveillanceSensor")


class SurveillanceHeartbeatEvent(Base):
    """Records each heartbeat dispatch for heartbeat rate and delivery probability metrics."""

    __tablename__ = "surveillance_heartbeat_event"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_session.id"))
    dispatched_at = Column(DateTime, default=datetime.utcnow, index=True)
    expected_at = Column(DateTime, nullable=False)
    delivered_on_time = Column(Boolean, default=True)

    session = relationship("SurveillanceSession", back_populates="heartbeat_events")

    def __str__(self):
        return f"HeartbeatEvent session={self.session_id} at {self.dispatched_at}"


class SurveillanceTrackEvent(Base):
    """Records each track-task execution outcome for track update probability metrics."""

    __tablename__ = "surveillance_track_event"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_session.id"))
    dispatched_at = Column(DateTime, default=datetime.utcnow, index=True)
    expected_at = Column(DateTime, nullable=False)
    had_active_tracks = Column(Boolean, default=False)

    session = relationship("SurveillanceSession", back_populates="track_events")

    def __str__(self):
        return f"TrackEvent session={self.session_id} at {self.dispatched_at}"


class SurveillanceSensorFailureNotification(Base):
    """Persists sensor failure and recovery events for audit and notification purposes."""

    __tablename__ = "surveillance_sensor_failure_notification"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_id = Column(UUID(as_uuid=True), ForeignKey("surveillance_sensor.id"))
    previous_status = Column(String(12), nullable=False)
    new_status = Column(String(12), nullable=False)
    recovery_type = Column(String(12), nullable=True)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    sensor = relationship("SurveillanceSensor")

    def __str__(self):
        return f"FailureNotification sensor={self.sensor_id} {self.previous_status}->{self.new_status}"
