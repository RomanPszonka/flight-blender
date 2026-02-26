import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, Float, Integer, String, Text

from flight_blender.db import Base


class SignedTelmetryPublicKey(Base):
    __tablename__ = "signed_telmetry_public_key"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    key_id = Column(Text, nullable=False)
    url = Column(String(200), nullable=False)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __str__(self):
        return "Key : " + self.url


class FlightObservation(Base):
    __tablename__ = "flight_observation"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), nullable=True)
    latitude_dd = Column(Float, nullable=False)
    longitude_dd = Column(Float, nullable=False)
    altitude_mm = Column(Float, nullable=False)
    traffic_source = Column(Integer, nullable=False)
    source_type = Column(Integer, nullable=False)
    icao_address = Column(Text, nullable=False)

    metadata_ = Column("metadata", Text, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
