import itertools
import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from flight_blender.db import Base


class FlightDeclaration(Base):
    __tablename__ = "flight_declaration"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operational_intent = Column(Text, nullable=False)
    flight_declaration_raw_geojson = Column(Text, nullable=True)
    type_of_operation = Column(Integer, default=1)
    bounds = Column(String(140), nullable=False)
    aircraft_id = Column(String(256), nullable=False)
    state = Column(Integer, default=0)

    originating_party = Column(String(100), default="Flight Blender Default")

    submitted_by = Column(String(254), nullable=True)
    approved_by = Column(String(254), nullable=True)

    latest_telemetry_datetime = Column(DateTime, nullable=True)

    start_datetime = Column(DateTime, default=datetime.now)
    end_datetime = Column(DateTime, default=datetime.now)

    is_approved = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tracking_info = relationship("FlightOperationTracking", back_populates="flight_declaration")

    def add_state_history_entry(self, original_state: int, new_state: int, notes: str = "", **kwargs):
        """Add a history tracking entry for this FlightDeclaration."""
        original_state = original_state or "start"
        deltas = {"original_state": str(original_state), "new_state": str(new_state)}

        entry = FlightOperationTracking(
            flight_declaration_id=self.id,
            notes=notes,
            deltas=deltas,
        )
        return entry

    def __unicode__(self):
        return self.originating_party + " " + str(self.id)

    def __str__(self):
        return self.originating_party + " " + str(self.id)


class FlightOperationalIntentDetail(Base):
    __tablename__ = "flight_operational_intent_detail"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), unique=True)

    volumes = Column(Text, default="")
    off_nominal_volumes = Column(Text, default="")
    priority = Column(Integer, nullable=False)
    subscribers = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    is_live = Column(Boolean, default=False)

    declaration = relationship("FlightDeclaration")


class FlightOperationalIntentReference(Base):
    __tablename__ = "flight_operational_intent_reference"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), unique=True)
    uss_availability = Column(String(256), nullable=False)

    ovn = Column(String(128), nullable=True)

    manager = Column(String(256), nullable=False)
    uss_base_url = Column(String(256), nullable=False)
    version = Column(String(256), nullable=False)
    state = Column(String(40), nullable=False)
    time_start = Column(DateTime, default=datetime.now)
    time_end = Column(DateTime, default=datetime.now)
    subscription_id = Column(String(256), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    is_live = Column(Boolean, default=False)

    declaration = relationship("FlightDeclaration")
    subscribers = relationship("Subscriber", back_populates="operational_intent_reference")


class Subscriber(Base):
    __tablename__ = "subscriber"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operational_intent_reference_id = Column(UUID(as_uuid=True), ForeignKey("flight_operational_intent_reference.id"))
    subscriptions = Column(Text, default="")
    uss_base_url = Column(String(256), nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    operational_intent_reference = relationship("FlightOperationalIntentReference", back_populates="subscribers")


class CompositeOperationalIntent(Base):
    __tablename__ = "composite_operational_intent"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), unique=True)
    bounds = Column(String(140), nullable=False)
    start_datetime = Column(DateTime, default=datetime.now)
    end_datetime = Column(DateTime, default=datetime.now)
    alt_max = Column(Float, nullable=False)
    alt_min = Column(Float, nullable=False)
    operational_intent_details_id = Column(UUID(as_uuid=True), ForeignKey("flight_operational_intent_detail.id"))
    operational_intent_reference_id = Column(UUID(as_uuid=True), ForeignKey("flight_operational_intent_reference.id"))

    declaration = relationship("FlightDeclaration")
    operational_intent_details = relationship("FlightOperationalIntentDetail")
    operational_intent_reference = relationship("FlightOperationalIntentReference")


class PeerOperationalIntentDetail(Base):
    """Store the details of the operational intent shared by the peer USS"""

    __tablename__ = "peer_operational_intent_detail"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    volumes = Column(Text, default="")
    off_nominal_volumes = Column(Text, default="")
    priority = Column(Integer, nullable=False)
    subscribers = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    is_live = Column(Boolean, default=False)


class PeerOperationalIntentReference(Base):
    """Store the details of the operational intent shared by the peer USS"""

    __tablename__ = "peer_operational_intent_reference"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    uss_availability = Column(String(256), nullable=False)

    ovn = Column(String(128), nullable=True)

    manager = Column(String(256), nullable=False)
    uss_base_url = Column(String(256), nullable=False)
    version = Column(String(256), nullable=False)
    state = Column(String(40), nullable=False)
    time_start = Column(DateTime, default=datetime.now)
    time_end = Column(DateTime, default=datetime.now)
    subscription_id = Column(String(256), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    is_live = Column(Boolean, default=False)


class PeerCompositeOperationalIntent(Base):
    __tablename__ = "peer_composite_operational_intent"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    bounds = Column(String(140), nullable=False)
    start_datetime = Column(DateTime, default=datetime.now)
    end_datetime = Column(DateTime, default=datetime.now)
    alt_max = Column(Float, nullable=False)
    alt_min = Column(Float, nullable=False)
    operational_intent_details_id = Column(UUID(as_uuid=True), ForeignKey("peer_operational_intent_detail.id"))
    operational_intent_reference_id = Column(UUID(as_uuid=True), ForeignKey("peer_operational_intent_reference.id"))

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    operational_intent_details = relationship("PeerOperationalIntentDetail")
    operational_intent_reference = relationship("PeerOperationalIntentReference")


class FlightOperationTracking(Base):
    __tablename__ = "flight_operation_tracking"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flight_declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"))

    notes = Column(String(512), nullable=True)

    deltas = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    flight_declaration = relationship("FlightDeclaration", back_populates="tracking_info")

    def __unicode__(self):
        return self.flight_declaration if self.flight_declaration else ""

    def __str__(self):
        return str(self.flight_declaration) if self.flight_declaration else ""
