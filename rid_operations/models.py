import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, Integer, String, Text

from flight_blender.db import Base


class ISASubscription(Base):
    __tablename__ = "isa_subscription"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    subscription_id = Column(UUID(as_uuid=True), default=uuid.uuid4, index=True)
    view = Column(Text, nullable=True)
    flight_details = Column(Text, nullable=True)
    end_datetime = Column(DateTime, nullable=True)
    view_hash = Column(Integer, nullable=True, index=True)
    is_simulated = Column(Boolean, default=False, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RIDFlightDetail(Base):
    __tablename__ = "rid_flight_detail"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_description = Column(Text, nullable=True)
    operator_location = Column(Text, nullable=True)
    operator_id = Column(String(255), nullable=True)
    auth_data = Column(String(255), nullable=True)
    uas_id = Column(String(255), nullable=True)
    eu_classification = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
