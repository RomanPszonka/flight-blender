import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import relationship

from flight_blender.db import Base


class ConstraintDetail(Base):
    __tablename__ = "constraint_detail"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    geofence_id = Column(UUID(as_uuid=True), ForeignKey("geo_fence.id"), unique=True, nullable=True)
    volumes = Column(Text, default="")
    _type = Column(String(256), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    geofence = relationship("GeoFence")


class ConstraintReference(Base):
    __tablename__ = "constraint_reference"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flight_declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), nullable=True)
    geofence_id = Column(UUID(as_uuid=True), ForeignKey("geo_fence.id"), unique=True, nullable=True)
    uss_availability = Column(String(40), default="")

    ovn = Column(String(128), nullable=True)

    manager = Column(String(256), nullable=True)
    uss_base_url = Column(String(256), default="")
    version = Column(String(256), default="")
    time_start = Column(DateTime, default=datetime.now)
    time_end = Column(DateTime, default=datetime.now)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    is_live = Column(Boolean, default=False)

    flight_declaration = relationship("FlightDeclaration")
    geofence = relationship("GeoFence")


class CompositeConstraint(Base):
    __tablename__ = "composite_constraint"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"))
    bounds = Column(String(140), nullable=False)
    start_datetime = Column(DateTime, default=datetime.now)
    end_datetime = Column(DateTime, default=datetime.now)
    alt_max = Column(Float, nullable=False)
    alt_min = Column(Float, nullable=False)
    constraint_reference_id = Column(UUID(as_uuid=True), ForeignKey("constraint_reference.id"))
    constraint_detail_id = Column(UUID(as_uuid=True), ForeignKey("constraint_detail.id"))

    declaration = relationship("FlightDeclaration")
    constraint_reference = relationship("ConstraintReference")
    constraint_detail = relationship("ConstraintDetail")
