from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from uuid import UUID

from implicitdict import ImplicitDict
from uas_standards.interuss.automated_testing.rid.v1.injection import (
    Time,
    UserNotification,
)

## This is the new RID telemetery data class, this will include 2022 RID data formats
## For more information see the ASTM RID data definitions at : https://github.com/uastech/standards/blob/dd4016b09fc8cb98f30c2a17b5a088fb2995ab54/remoteid/canonical.yaml

# This file was generated from the ASTM OpenID implementation
# generated by datamodel-codegen:
#   filename:  canonical.yaml
#   timestamp: 2023-04-13T15:31:36+00:00


URL = str
SubscriptionNotificationIndex = int
UUIDv4 = str
Version = str
EntityUUID = UUIDv4
SubscriptionUUID = UUIDv4
RIDFlightID = str
Latitude = float
Longitude = float
SpecificSessionID = str
USSBaseURL = str
SubscriptionUSSBaseURL = USSBaseURL
FlightsUSSBaseURL = USSBaseURL


class Format(str, Enum):
    RFC3339 = "RFC3339"


class Reference(Enum):
    TakeoffLocation = "TakeoffLocation"
    GroundLevel = "GroundLevel"


class SpeedAccuracy(str, Enum):
    SAUnknown = "SAUnknown"
    SA10mpsPlus = "SA10mpsPlus"
    SA10mps = "SA10mps"
    SA3mps = "SA3mps"
    SA1mps = "SA1mps"
    SA03mps = "SA03mps"


class HorizontalAccuracy(str, Enum):
    HAUnknown = "HAUnknown"
    HA10NMPlus = "HA10NMPlus"
    HA10NM = "HA10NM"
    HA4NM = "HA4NM"
    HA2NM = "HA2NM"
    HA1NM = "HA1NM"
    HA05NM = "HA05NM"
    HA03NM = "HA03NM"
    HA01NM = "HA01NM"
    HA005NM = "HA005NM"
    HA30m = "HA30m"
    HA10m = "HA10m"
    HA3m = "HA3m"
    HA1m = "HA1m"


class VerticalAccuracy(str, Enum):
    VAUnknown = "VAUnknown"
    VA150mPlus = "VA150mPlus"
    VA150m = "VA150m"
    VA45m = "VA45m"
    VA25m = "VA25m"
    VA10m = "VA10m"
    VA3m = "VA3m"
    VA1m = "VA1m"


class RIDOperationalStatus(str, Enum):
    Undeclared = "Undeclared"
    Ground = "Ground"
    Airborne = "Airborne"
    Emergency = "Emergency"
    RemoteIDSystemFailure = "RemoteIDSystemFailure"


class AltitudeType(str, Enum):
    Takeoff = "Takeoff"
    Dynamic = "Dynamic"
    Fixed = "Fixed"


class Category(str, Enum):
    EUCategoryUndefined = "EUCategoryUndefined"
    Open = "Open"
    Specific = "Specific"
    Certified = "Certified"


class Class(str, Enum):
    EUClassUndefined = "EUClassUndefined"
    Class0 = "Class0"
    Class1 = "Class1"
    Class2 = "Class2"
    Class3 = "Class3"
    Class4 = "Class4"
    Class5 = "Class5"
    Class6 = "Class6"


class UAType(str, Enum):
    NotDeclared = "NotDeclared"
    Aeroplane = "Aeroplane"
    Helicopter = "Helicopter"
    Gyroplane = "Gyroplane"
    HybridLift = "HybridLift"
    Ornithopter = "Ornithopter"
    Glider = "Glider"
    Kite = "Kite"
    FreeBalloon = "FreeBalloon"
    CaptiveBalloon = "CaptiveBalloon"
    Airship = "Airship"
    FreeFallOrParachute = "FreeFallOrParachute"
    Rocket = "Rocket"
    TetheredPoweredAircraft = "TetheredPoweredAircraft"
    GroundObstacle = "GroundObstacle"
    Other = "Other"


class Units(str, Enum):
    M = "M"


@dataclass
class Time:
    value: str
    format: str = "RFC3339"


@dataclass
class Radius:
    value: float
    units: Units


@dataclass
class RIDAuthData:
    data: Optional[str] = ""
    format: Optional[int] = 0


@dataclass
class ErrorResponse:
    message: Optional[str] = ""


GeoPolygonString = str


@dataclass
class RIDHeight:
    reference: Reference
    distance: Optional[float] = 0


@dataclass
class LatLngPoint:
    lng: Longitude
    lat: Latitude


class Reference1(Enum):
    W84 = "W84"


@dataclass
class Altitude:
    value: float
    reference: Reference1
    units: Units


@dataclass
class OperatingArea:
    aircraft_count: Optional[int] = None
    volumes: Optional[List[OperatingArea]] = list


@dataclass
class Polygon:
    vertices: List[LatLngPoint]


@dataclass
class UASID:
    specific_session_id: Optional[SpecificSessionID] = None
    serial_number: Optional[str] = ""
    registration_id: Optional[str] = ""
    utm_id: Optional[str] = ""


@dataclass
class OperatorLocation:
    position: LatLngPoint
    altitude: Optional[Altitude] = None
    altitude_type: Optional[AltitudeType] = None


@dataclass
class UAClassificationEU:
    category: Optional[Category] = "EUCategoryUndefined"
    class_: Optional[Class] = "EUClassUndefined"


@dataclass
class RIDFlightDetails:
    id: str
    eu_classification: Optional[UAClassificationEU] = None
    uas_id: Optional[UASID] = None
    operator_location: Optional[LatLngPoint] = None
    auth_data: Optional[RIDAuthData] = None
    operator_id: Optional[str] = ""
    operation_description: Optional[str] = ""


@dataclass
class Circle:
    center: Optional[LatLngPoint] = None
    radius: Optional[Radius] = None


@dataclass
class Volume3D:
    outline_circle: Optional[Circle] = None
    outline_polygon: Optional[Polygon] = None
    altitude_lower: Optional[Altitude] = None
    altitude_upper: Optional[Altitude] = None


@dataclass
class Volume4D:
    volume: Volume3D
    time_start: Optional[Time] = None
    time_end: Optional[Time] = None


@dataclass
class SubscriptionState:
    subscription_id: SubscriptionUUID
    notification_index: Optional[SubscriptionNotificationIndex] = 0


@dataclass
class GetFlightDetailsResponse:
    details: RIDFlightDetails


@dataclass
class RIDAircraftPosition:
    lat: Latitude
    lng: Longitude
    accuracy_h: HorizontalAccuracy
    accuracy_v: VerticalAccuracy
    height: Optional[RIDHeight]
    alt: Optional[float] = -1000
    pressure_altitude: Optional[float] = -1000
    extrapolated: Optional[bool] = False


@dataclass
class SubscriberToNotify:
    subscriptions: List[SubscriptionState]
    url: URL


@dataclass
class RIDRecentAircraftPosition:
    time: Time
    position: RIDAircraftPosition


@dataclass
class GetIdentificationServiceAreaDetailsResponse:
    extents: Volume4D


@dataclass
class CreateIdentificationServiceAreaParameters:
    extents: Volume4D
    uss_base_url: FlightsUSSBaseURL


@dataclass
class UpdateIdentificationServiceAreaParameters:
    extents: Volume4D
    uss_base_url: FlightsUSSBaseURL


@dataclass
class CreateSubscriptionParameters:
    extents: Volume4D
    uss_base_url: SubscriptionUSSBaseURL


@dataclass
class UpdateSubscriptionParameters:
    extents: Volume4D
    uss_base_url: SubscriptionUSSBaseURL


@dataclass
class Subscription:
    id: SubscriptionUUID
    uss_base_url: SubscriptionUSSBaseURL
    owner: str
    version: Version
    time_end: Optional[Time]
    time_start: Optional[Time][SubscriptionNotificationIndex] = 0
    notification_index: Optional[int] = 0


@dataclass
class IdentificationServiceArea:
    uss_base_url: FlightsUSSBaseURL
    owner: str
    time_start: Time
    time_end: Time
    version: Version
    id: EntityUUID


@dataclass
class RIDAircraftState:
    timestamp: Time
    timestamp_accuracy: float
    position: RIDAircraftPosition
    speed_accuracy: SpeedAccuracy
    operational_status: Optional[RIDOperationalStatus] = "Undeclared"
    speed: Optional[float] = 255
    track: Optional[float] = 361
    vertical_speed: Optional[float] = 63


@dataclass
class SignedUnsignedTelemetryObservation:
    current_state: RIDAircraftState
    flight_details: RIDFlightDetails


@dataclass
class SignedUnSignedTelemetryObservations:
    current_states: List[RIDAircraftState]
    flight_details: RIDFlightDetails


@dataclass
class SignedTelemetryRequest:
    observations: List[SignedUnsignedTelemetryObservation]


@dataclass
class SubmittedTelemetryFlightDetails:
    id: str
    aircraft_type: str
    current_state: RIDAircraftState
    simulated: bool
    recent_positions: List[RIDRecentAircraftPosition]
    operator_details: RIDFlightDetails


@dataclass
class RIDStreamErrorDetail:
    error_code: int
    error_description: str


class ServiceProviderUserNotifications(ImplicitDict):
    user_notifications: list[UserNotification] = []


class OperatorRIDNotificationCreationPayload(ImplicitDict):
    message: str
    session_id: UUID
