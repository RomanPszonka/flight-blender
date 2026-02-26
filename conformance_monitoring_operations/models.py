import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from flight_blender.db import Base


class ConformanceRecord(Base):
    __tablename__ = "conformance_record"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flight_declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"))
    conformance_state = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    description = Column(Text, nullable=False)
    event_type = Column(String(20), nullable=False)
    geofence_breach = Column(Boolean, default=False)
    geofence_id = Column(UUID(as_uuid=True), ForeignKey("geo_fence.id"), nullable=True)
    resolved = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    flight_declaration = relationship("FlightDeclaration")
    geofence = relationship("GeoFence")

    def __str__(self):
        return f"Conformance Record {self.id} for Flight Declaration {self.flight_declaration_id}"


class TaskScheduler(Base):
    __tablename__ = "task_scheduler"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    periodic_task_name = Column(String(256), nullable=True)
    flight_declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), unique=True, nullable=True)
    session_id = Column(UUID(as_uuid=True), nullable=True)

    flight_declaration = relationship("FlightDeclaration")
