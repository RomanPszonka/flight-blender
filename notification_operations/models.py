import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship

from flight_blender.db import Base


class OperatorRIDNotification(Base):
    __tablename__ = "operator_rid_notification"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), default=uuid.uuid4, nullable=True)
    message = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    flight_declaration_id = Column(UUID(as_uuid=True), ForeignKey("flight_declaration.id"), nullable=True)

    flight_declaration = relationship("FlightDeclaration")

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
