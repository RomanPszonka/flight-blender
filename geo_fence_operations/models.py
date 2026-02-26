import uuid
from datetime import datetime

from sqlalchemy import UUID, Boolean, Column, DateTime, Integer, Numeric, String, Text

from flight_blender.db import Base

STATUS_CODES = (
    (0, "Activating"),
    (1, "Ready"),
    (3, "Deactivating"),
    (4, "Unsupported"),
    (5, "Rejected"),
    (6, "Error"),
)


class GeoFence(Base):
    """A model for Geofence storage in Flight Blender"""

    __tablename__ = "geo_fence"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    raw_geo_fence = Column(Text, nullable=True)

    geozone = Column(Text, nullable=True)

    upper_limit = Column(Numeric(precision=6, scale=2), nullable=False)
    lower_limit = Column(Numeric(precision=6, scale=2), nullable=False)

    altitude_ref = Column(Integer, default=0)

    name = Column(String(50), nullable=False)
    bounds = Column(String(140), nullable=False)

    status = Column(Integer, default=0)
    message = Column(String(140), nullable=True)

    is_test_dataset = Column(Boolean, default=False)

    start_datetime = Column(DateTime, default=datetime.now)
    end_datetime = Column(DateTime, default=datetime.now)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __unicode__(self):
        return self.name

    def __str__(self):
        return self.name
