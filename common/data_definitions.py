from os import environ as env

FLIGHTBLENDER_READ_SCOPE = env.get("FLIGHTBLENDER_READ_SCOPE", "flightblender.read")

FLIGHTBLENDER_WRITE_SCOPE = env.get("FLIGHTBLENDER_WRITE_SCOPE", "flightblender.write")


ALTITUDE_REF = (
    (0, "WGS84"),
    (1, "AGL"),
    (2, "MSL"),
    (4, "W84"),
)
ALTITUDE_REF_LOOKUP = {
    "WGS84": 0,
    "AGL": 1,
    "MSL": 2,
    "W84": 4,
}
CONFORMANCE_STATES = (
    (0, "Nonconforming"),
    (1, "Conforming"),
)
OPERATION_STATES = (
    (0, "Not Submitted"),
    (1, "Accepted"),
    (2, "Activated"),
    (3, "Nonconforming"),
    (4, "Contingent"),
    (5, "Ended"),
    (6, "Withdrawn"),
    (7, "Cancelled"),
    (8, "Rejected"),
)
ACTIVE_OPERATIONAL_STATES = [1, 2, 3, 4]

# This is only used int he SCD Test harness therefore it is partial
OPERATION_STATES_LOOKUP = {
    "Accepted": 1,
    "Activated": 2,
}

OPERATION_TYPES = (
    (1, "VLOS"),
    (2, "BVLOS"),
    (3, "CREWED"),
)

USS_AVAILABILITY = (
    (0, "Unknown"),
    (1, "Normal"),
    (2, "Down"),
)

# When an operator changes a state, he / she puts a new state (via the API), this object specifies the event when a operator takes action
OPERATOR_EVENT_LOOKUP = {
    5: "operator_confirms_ended",
    2: "operator_activates",
    4: "operator_initiates_contingent",
}

VALID_OPERATIONAL_INTENT_STATES = [
    "Accepted",
    "Activated",
    "Nonconforming",
    "Contingent",
]


RESPONSE_CONTENT_TYPE = "application/json"


FLIGHT_OBSERVATION_TRAFFIC_SOURCE = (
    (0, "1090ES"),
    (1, "UAT"),
    (2, "Multi-radar (MRT)"),
    (3, "MLAT"),
    (4, "SSR"),
    (5, "PSR"),
    (6, "Mode-S"),
    (7, "MRT"),
    (8, "SSR + PSR Fused"),
    (9, "ADS-B"),
    (10, "FLARM"),
    (11, "Network Remote-ID"),
    (12, "Other"),
)


SURVEILLANCE_SENSOR_HEALTH_CHOICES = [
    ("operational", "Operational"),
    ("degraded", "Degraded"),
    ("outage", "Outage"),
]

SURVEILLANCE_SENSOR_MAINTENANCE_CHOICES = [
    ("planned", "Planned"),
    ("unplanned", "Unplanned"),
]

# Locations for Index Creation, using tmp to avoid permission issues in Docker / Kubernetes
FLIGHT_DECLARATION_INDEX_BASEPATH = "/tmp/blender_flight_declaration_idx"
FLIGHT_DECLARATION_OPINT_INDEX_BASEPATH = "/tmp/blender_opint_idx"
GEOFENCE_INDEX_BASEPATH = "/tmp/blender_geofence_idx"
OPINT_INDEX_BASEPATH = "/tmp/blender_opint_proc_idx"

DEFAULT_UAV_SPEED_M_PER_S = env.get("DEFAULT_UAV_SPEED_M_PER_S", 5.5)  # ~20 km/h
DEFAULT_UAV_CLIMB_RATE_M_PER_S = env.get("DEFAULT_UAV_CLIMB_RATE_M_PER_S", 2.0)  # ~7.2 km/h
DEFAULT_UAV_DESCENT_RATE_M_PER_S = env.get("DEFAULT_UAV_DESCENT_RATE_M_PER_S", 2.0)  # ~7.2 km/h
