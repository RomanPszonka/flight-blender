import json
import os
import uuid
from dataclasses import asdict
from datetime import datetime
from typing import Never, Optional
from uuid import UUID

import arrow
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from common.utils import EnhancedJSONEncoder
from conformance_monitoring_operations.models import ConformanceRecord, TaskScheduler
from constraint_operations.data_definitions import (
    CompositeConstraintPayload,
    ConstraintDetails,
)
from constraint_operations.data_definitions import Constraint as ConstraintData
from constraint_operations.data_definitions import (
    ConstraintReference as ConstraintReferencePayload,
)
from constraint_operations.models import (
    CompositeConstraint,
    ConstraintDetail,
    ConstraintReference,
)
from flight_declaration_operations.models import (
    CompositeOperationalIntent,
    FlightDeclaration,
    FlightOperationalIntentDetail,
    FlightOperationalIntentReference,
    PeerCompositeOperationalIntent,
    PeerOperationalIntentDetail,
    PeerOperationalIntentReference,
    Subscriber,
)
from flight_feed_operations.data_definitions import SingleAirtrafficObservation
from flight_feed_operations.models import FlightObservation
from geo_fence_operations.data_definitions import GeofencePayload
from geo_fence_operations.models import GeoFence
from notification_operations.models import OperatorRIDNotification
from rid_operations.data_definitions import OperatorRIDNotificationCreationPayload
from rid_operations.models import ISASubscription, RIDFlightDetail
from rid_operations.rid_utils import RIDFlightDetails
from scd_operations.data_definitions import FlightDeclarationCreationPayload
from scd_operations.scd_data_definitions import (
    CompositeOperationalIntentPayload,
    OperationalIntentReferenceDSSResponse,
    OperationalIntentStorage,
    OperationalIntentUSSDetails,
    PartialCreateOperationalIntentReference,
    SubscriberToNotify,
)
from surveillance_monitoring_operations.models import (
    SurveillanceHeartbeatEvent,
    SurveillanceSensor,
    SurveillanceSensorFailureNotification,
    SurveillanceSensorHealth,
    SurveillanceSensorMaintenance,
    SurveillanceSensortHealthTracking,
    SurveillanceSession,
    SurveillanceTrackEvent,
)

load_dotenv(find_dotenv())

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)


class FlightBlenderDatabaseReader:
    """
    A file to unify read and write operations to the database. Eventually caching etc. can be added via this file
    """

    def __init__(self, db: Session):
        self.db = db

    def get_peer_operational_intent_details_by_id(self, operational_intent_id: str) -> None | PeerOperationalIntentDetail:
        return self.db.query(PeerOperationalIntentDetail).filter(PeerOperationalIntentDetail.id == operational_intent_id).first()

    def get_peer_operational_intent_reference_by_id(self, operational_intent_reference_id: str) -> None | PeerOperationalIntentReference:
        return self.db.query(PeerOperationalIntentReference).filter(PeerOperationalIntentReference.id == operational_intent_reference_id).first()

    def check_constraint_id_exists(self, constraint_id: str) -> bool:
        return self.db.query(ConstraintDetail).filter(ConstraintDetail.id == constraint_id).first() is not None

    def get_constraint_by_geofence(self, geofence: GeoFence) -> list[ConstraintDetail]:
        return self.db.query(ConstraintDetail).filter(ConstraintDetail.geofence_id == geofence.id).all()

    def check_constraint_reference_id_exists(self, constraint_reference_id: str) -> bool:
        return self.db.query(ConstraintReference).filter(ConstraintReference.id == constraint_reference_id).first() is not None

    def get_constraint_reference_by_id(self, constraint_reference_id: str) -> ConstraintReference:
        return self.db.query(ConstraintReference).filter(ConstraintReference.id == constraint_reference_id).first()

    def get_constraint_details(self, constraint_id: str) -> ConstraintDetail:
        return self.db.query(ConstraintDetail).filter(ConstraintDetail.id == constraint_id).first()

    def get_flight_observations(self, after_datetime: arrow.arrow.Arrow):
        observations = (
            self.db.query(FlightObservation)
            .filter(FlightObservation.created_at >= after_datetime.isoformat())
            .order_by(FlightObservation.created_at)
            .all()
        )
        return observations

    def get_closest_flight_observation_for_now(self, now: arrow.arrow.Arrow):
        one_second_before_now = now.shift(seconds=-1)

        observations = self.db.query(FlightObservation).filter(
            FlightObservation.created_at >= one_second_before_now.isoformat(),
            FlightObservation.created_at <= now.isoformat(),
        ).all()
        return observations

    def get_flight_observation_objects(self):
        observations = self.db.query(FlightObservation).order_by(FlightObservation.created_at).all()
        return observations

    def get_temporal_flight_observations_by_session(self, session_id: str, after_datetime: arrow.arrow.Arrow):
        observations = (
            self.db.query(FlightObservation)
            .filter(
                FlightObservation.session_id == session_id,
                FlightObservation.created_at >= after_datetime.isoformat(),
            )
            .order_by(FlightObservation.created_at)
            .all()
        )
        return observations

    def get_flight_observations_by_session(self, session_id: str, after_datetime: arrow.arrow.Arrow):
        observations = (
            self.db.query(FlightObservation)
            .filter(
                FlightObservation.session_id == session_id,
                FlightObservation.created_at >= after_datetime.isoformat(),
                FlightObservation.traffic_source != 11,
            )
            .order_by(FlightObservation.created_at)
            .all()
        )
        return observations

    def get_latest_flight_observation_by_session(self, session_id: str):
        observation = (
            self.db.query(FlightObservation)
            .filter(FlightObservation.session_id == session_id)
            .order_by(FlightObservation.created_at.desc())
            .first()
        )
        return observation

    def get_all_flight_declarations(self) -> list[FlightDeclaration]:
        flight_declarations = self.db.query(FlightDeclaration).all()
        return flight_declarations

    def check_flight_declaration_exists(self, flight_declaration_id: str) -> bool:
        return self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first() is not None

    def get_flight_declaration_by_id(self, flight_declaration_id: str) -> None | FlightDeclaration:
        return self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()

    def check_composite_operational_intent_exists(self, flight_declaration_id: str) -> bool:
        return self.db.query(CompositeOperationalIntent).filter(CompositeOperationalIntent.declaration_id == flight_declaration_id).first() is not None

    def get_composite_operational_intent_by_declaration_id(self, flight_declaration_id: str) -> None | CompositeOperationalIntent:
        return self.db.query(CompositeOperationalIntent).filter(CompositeOperationalIntent.declaration_id == flight_declaration_id).first()

    def get_flight_operational_intent_reference_by_flight_declaration_id(self, flight_declaration_id: str) -> None | FlightOperationalIntentReference:
        """
        Retrieves a FlightAuthorization object based on the given flight declaration ID.
        Args:
            flight_declaration_id (str): The ID of the flight declaration.
        Returns:
            Union[None, FlightAuthorization]: The FlightAuthorization object if found, otherwise None.
        Raises:
            FlightDeclaration.DoesNotExist: If the flight declaration with the given ID does not exist.
            FlightAuthorization.DoesNotExist: If the flight authorization for the given flight declaration does not exist.
        """

        flight_declaration = self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()
        if flight_declaration is None:
            return None
        flight_operational_intent_reference = (
            self.db.query(FlightOperationalIntentReference)
            .filter(FlightOperationalIntentReference.declaration_id == flight_declaration.id)
            .first()
        )
        return flight_operational_intent_reference

    def get_active_geofences(self) -> None | list[GeoFence]:
        now = arrow.now()

        return self.db.query(GeoFence).filter(
            GeoFence.start_datetime <= now.isoformat(),
            GeoFence.end_datetime >= now.isoformat(),
        ).all()

    def get_flight_operational_intent_reference_by_flight_declaration_obj(
        self, flight_declaration: FlightDeclaration
    ) -> None | FlightOperationalIntentReference:
        return (
            self.db.query(FlightOperationalIntentReference)
            .filter(FlightOperationalIntentReference.declaration_id == flight_declaration.id)
            .first()
        )

    def check_flight_operational_intent_reference_by_id_exists(self, operational_intent_ref_id: str) -> bool:
        return self.db.query(FlightOperationalIntentReference).filter(FlightOperationalIntentReference.id == operational_intent_ref_id).first() is not None

    def get_operational_intent_reference_by_id(self, operational_intent_ref_id: str) -> FlightOperationalIntentReference:
        return self.db.query(FlightOperationalIntentReference).filter(FlightOperationalIntentReference.id == operational_intent_ref_id).first()

    def get_flight_operational_intent_reference_by_id(self, operational_intent_ref_id: str) -> None | FlightOperationalIntentReference:
        """
        Retrieves a FlightOperationalIntentReference object based on the given flight declaration ID.
        Args:
            flight_declaration_id (str): The ID of the flight declaration.
        Returns:
            Union[None, FlightOperationalIntentReference]: The FlightOperationalIntentReference object if found, otherwise None.
        Raises:
            FlightDeclaration.DoesNotExist: If the flight declaration with the given ID does not exist.
            FlightOperationalIntentReference.DoesNotExist: If the flight authorization for the given flight declaration does not exist.
        """

        return self.db.query(FlightOperationalIntentReference).filter(FlightOperationalIntentReference.id == operational_intent_ref_id).first()

    def get_operational_intent_details_by_flight_declaration(self, flight_declaration: FlightDeclaration) -> None | FlightOperationalIntentDetail:
        return (
            self.db.query(FlightOperationalIntentDetail)
            .filter(FlightOperationalIntentDetail.declaration_id == flight_declaration.id)
            .first()
        )

    def update_flight_operational_intent_reference_ovn(
        self,
        flight_operational_intent_referecne: FlightOperationalIntentReference,
        ovn: str,
    ) -> bool:
        try:
            flight_operational_intent_referecne.ovn = ovn
            self.db.add(flight_operational_intent_referecne)
            self.db.commit()
            self.db.refresh(flight_operational_intent_referecne)
            return True

        except IntegrityError:
            self.db.rollback()
            return False

    def get_subscribers_of_operational_intent_reference(
        self, flight_operational_intent_reference: FlightOperationalIntentReference
    ) -> Never | list[Subscriber]:
        subscribers = (
            self.db.query(Subscriber)
            .filter(Subscriber.operational_intent_reference_id == flight_operational_intent_reference.id)
            .all()
        )
        return subscribers

    def check_flight_operational_intent_details_by_id_exists(self, operational_intent_ref_id: str) -> bool:
        return self.db.query(FlightOperationalIntentDetail).filter(FlightOperationalIntentDetail.id == operational_intent_ref_id).first() is not None

    def get_operational_intent_details_by_flight_declaration_id(self, declaration_id: str) -> None | FlightOperationalIntentDetail:
        """
        Retrieves a FlightOperationalIntentReference object based on the given flight declaration ID.
        Args:
            flight_declaration_id (str): The ID of the flight declaration.
        Returns:
            Union[None, FlightOperationalIntentReference]: The FlightOperationalIntentReference object if found, otherwise None.
        Raises:
            FlightDeclaration.DoesNotExist: If the flight declaration with the given ID does not exist.
            FlightOperationalIntentReference.DoesNotExist: If the flight authorization for the given flight declaration does not exist.
        """

        return self.db.query(FlightOperationalIntentDetail).filter(FlightOperationalIntentDetail.declaration_id == declaration_id).first()

    def get_geofence_by_constraint_reference_id(self, constraint_reference_id: str) -> None | GeoFence:
        constraint_reference = self.db.query(ConstraintReference).filter(ConstraintReference.id == constraint_reference_id).first()
        if constraint_reference is None:
            return None
        geofence = self.db.query(GeoFence).filter(GeoFence.id == constraint_reference.geofence_id).first()
        return geofence

    def get_conformance_records_for_duration(self, start_time: datetime, end_time: datetime) -> None | list[ConformanceRecord]:
        """
        Retrieves conformance records created within the specified time duration.
        This method queries the ConformanceRecord model to fetch records where the
        'created_at' field is between the given start_time and end_time (inclusive).
        The results are ordered by 'created_at' in descending order (most recent first).
        Args:
            start_time (datetime): The start of the time range for filtering records.
            end_time (datetime): The end of the time range for filtering records.
        Returns:
            None | list[ConformanceRecord]: A list of ConformanceRecord
            objects if records are found, or None if no records exist (though note that
            the exception handling may not trigger as expected for filter queries).
        """

        conformance_records = (
            self.db.query(ConformanceRecord)
            .filter(
                ConformanceRecord.created_at >= start_time,
                ConformanceRecord.created_at <= end_time,
            )
            .order_by(ConformanceRecord.created_at.desc())
            .all()
        )
        return conformance_records

    def get_conformance_record_by_flight_declaration(self, flight_declaration: FlightDeclaration) -> None | list[ConformanceRecord]:
        conformance_record = (
            self.db.query(ConformanceRecord)
            .filter(ConformanceRecord.flight_declaration_id == flight_declaration.id)
            .all()
        )
        return conformance_record

    def check_flight_declaration_active(self, flight_declaration_id: str, now: datetime) -> bool:
        return (
            self.db.query(FlightDeclaration)
            .filter(
                FlightDeclaration.id == flight_declaration_id,
                FlightDeclaration.start_datetime <= now,
                FlightDeclaration.end_datetime >= now,
            )
            .first()
            is not None
        )

    def check_active_activated_flights_exist(self) -> bool:
        return self.db.query(FlightDeclaration).filter(FlightDeclaration.state.in_([1, 2])).first() is not None

    def get_active_activated_flight_declarations(
        self,
    ) -> list[FlightDeclaration]:
        return self.db.query(FlightDeclaration).filter(FlightDeclaration.state.in_([1, 2])).all()

    def get_current_flight_accepted_activated_declaration_ids(self, now: str) -> list:
        """This method gets flight operation ids that are active in the system"""
        n = arrow.get(now)

        two_minutes_before_now = n.shift(seconds=-120).isoformat()
        five_hours_from_now = n.shift(minutes=300).isoformat()
        relevant_ids = [
            r[0]
            for r in self.db.query(FlightDeclaration.id)
            .filter(
                FlightDeclaration.start_datetime >= two_minutes_before_now,
                FlightDeclaration.end_datetime <= five_hours_from_now,
                FlightDeclaration.state.in_([1, 2]),
            )
            .all()
        ]
        return relevant_ids

    def check_flight_details_exist(self, flight_detail_id: str) -> bool:
        return self.db.query(RIDFlightDetail).filter(RIDFlightDetail.id == flight_detail_id).first() is not None

    def get_flight_details_by_id(self, flight_detail_id: str) -> RIDFlightDetail:
        return self.db.query(RIDFlightDetail).filter(RIDFlightDetail.id == flight_detail_id).first()

    def get_conformance_monitoring_task(self, flight_declaration: FlightDeclaration) -> None | TaskScheduler:
        return (
            self.db.query(TaskScheduler)
            .filter(TaskScheduler.flight_declaration_id == flight_declaration.id)
            .first()
        )

    def get_rid_monitoring_task(self, session_id: UUID) -> None | TaskScheduler:
        return self.db.query(TaskScheduler).filter(TaskScheduler.session_id == session_id).first()

    def get_active_rid_observations_for_view(self, start_time: datetime, end_time: datetime) -> None | list[FlightObservation]:
        observations = (
            self.db.query(FlightObservation)
            .filter(
                FlightObservation.created_at >= start_time,
                FlightObservation.created_at <= end_time,
                FlightObservation.traffic_source == 11,
            )
            .order_by(FlightObservation.created_at.desc())
            .all()
        )
        return observations

    def get_active_rid_observations_for_session(self, session_id: str) -> None | list[FlightObservation]:
        observations = (
            self.db.query(FlightObservation)
            .filter(
                FlightObservation.session_id == session_id,
                FlightObservation.traffic_source == 11,
            )
            .order_by(FlightObservation.created_at.desc())
            .all()
        )
        return observations

    def get_active_rid_observations_for_session_between_interval(
        self, start_time: datetime, end_time: datetime, session_id: str
    ) -> None | list[FlightObservation]:
        observations = (
            self.db.query(FlightObservation)
            .filter(
                FlightObservation.session_id == session_id,
                FlightObservation.created_at >= start_time,
                FlightObservation.created_at <= end_time,
                FlightObservation.traffic_source == 11,
            )
            .all()
        )
        return observations

    def get_active_surveillance_sensors(self) -> list[SurveillanceSensor]:
        return self.db.query(SurveillanceSensor).filter(SurveillanceSensor.is_active == True).all()  # noqa: E712

    def get_surveillance_sensor_by_id(self, sensor_id: UUID) -> SurveillanceSensor | None:
        return self.db.query(SurveillanceSensor).filter(SurveillanceSensor.id == sensor_id).first()

    def get_surveillance_session_by_id(self, session_id: str) -> None | SurveillanceSession:
        return self.db.query(SurveillanceSession).filter(SurveillanceSession.id == session_id).first()

    def get_surveillance_periodic_tasks_by_session_id(self, session_id: str) -> list[TaskScheduler]:
        return self.db.query(TaskScheduler).filter(TaskScheduler.session_id == session_id).all()

    def get_all_active_surveillance_sessions(self) -> list[SurveillanceSession]:
        now = arrow.now().datetime
        return self.db.query(SurveillanceSession).filter(SurveillanceSession.valid_until >= now).all()

    def get_sensor_health_record(self, sensor_id: str) -> Optional[SurveillanceSensorHealth]:
        return self.db.query(SurveillanceSensorHealth).filter(SurveillanceSensorHealth.sensor_id == sensor_id).first()

    def get_health_tracking_records_for_sensor(
        self, sensor_id: str, start_time: datetime, end_time: datetime
    ) -> list[SurveillanceSensortHealthTracking]:
        return (
            self.db.query(SurveillanceSensortHealthTracking)
            .filter(
                SurveillanceSensortHealthTracking.sensor_id == sensor_id,
                SurveillanceSensortHealthTracking.recorded_at >= start_time,
                SurveillanceSensortHealthTracking.recorded_at <= end_time,
            )
            .order_by(SurveillanceSensortHealthTracking.recorded_at)
            .all()
        )

    def get_sensor_status_before_time(self, sensor_id: str, before_time: datetime) -> Optional[str]:
        record = (
            self.db.query(SurveillanceSensortHealthTracking)
            .filter(
                SurveillanceSensortHealthTracking.sensor_id == sensor_id,
                SurveillanceSensortHealthTracking.recorded_at < before_time,
            )
            .order_by(SurveillanceSensortHealthTracking.recorded_at.desc())
            .first()
        )
        return record.status if record else None

    def get_heartbeat_events_for_session(self, session_id: str, start_time: datetime, end_time: datetime) -> list[SurveillanceHeartbeatEvent]:
        return (
            self.db.query(SurveillanceHeartbeatEvent)
            .filter(
                SurveillanceHeartbeatEvent.session_id == session_id,
                SurveillanceHeartbeatEvent.dispatched_at >= start_time,
                SurveillanceHeartbeatEvent.dispatched_at <= end_time,
            )
            .order_by(SurveillanceHeartbeatEvent.dispatched_at)
            .all()
        )

    def get_track_events_for_session(self, session_id: str, start_time: datetime, end_time: datetime) -> list[SurveillanceTrackEvent]:
        return (
            self.db.query(SurveillanceTrackEvent)
            .filter(
                SurveillanceTrackEvent.session_id == session_id,
                SurveillanceTrackEvent.dispatched_at >= start_time,
                SurveillanceTrackEvent.dispatched_at <= end_time,
            )
            .order_by(SurveillanceTrackEvent.dispatched_at)
            .all()
        )

    def get_failure_notifications_for_sensor(
        self, sensor_id: str, start_time: datetime, end_time: datetime
    ) -> list[SurveillanceSensorFailureNotification]:
        return (
            self.db.query(SurveillanceSensorFailureNotification)
            .filter(
                SurveillanceSensorFailureNotification.sensor_id == sensor_id,
                SurveillanceSensorFailureNotification.created_at >= start_time,
                SurveillanceSensorFailureNotification.created_at <= end_time,
            )
            .order_by(SurveillanceSensorFailureNotification.created_at.desc())
            .all()
        )

    def get_active_user_notifications_between_interval(
        self, start_time: datetime, end_time: datetime
    ) -> None | list[OperatorRIDNotification]:
        notifications = (
            self.db.query(OperatorRIDNotification)
            .filter(
                OperatorRIDNotification.created_at >= start_time,
                OperatorRIDNotification.created_at <= end_time,
                OperatorRIDNotification.is_active == True,  # noqa: E712
            )
            .all()
        )
        return notifications

    def check_rid_subscription_record_by_view_hash_exists(self, view_hash: int) -> bool:
        return self.db.query(ISASubscription).filter(ISASubscription.view_hash == view_hash).first() is not None

    def check_rid_subscription_record_by_subscription_id_exists(self, subscription_id: str) -> bool:
        return self.db.query(ISASubscription).filter(ISASubscription.subscription_id == subscription_id).first() is not None

    def get_rid_subscription_record_by_subscription_id(self, subscription_id: str) -> ISASubscription:
        return self.db.query(ISASubscription).filter(ISASubscription.subscription_id == subscription_id).first()

    def get_all_rid_simulated_subscription_records(self) -> list[ISASubscription]:
        now = arrow.now().datetime
        return (
            self.db.query(ISASubscription)
            .filter(
                ISASubscription.is_simulated == True,  # noqa: E712
                ISASubscription.end_datetime >= now,
                ISASubscription.created_at <= now,
            )
            .all()
        )

    def get_rid_subscription_record_by_id(self, id: str) -> ISASubscription:
        return self.db.query(ISASubscription).filter(ISASubscription.id == id).first()


class FlightBlenderDatabaseWriter:
    def __init__(self, db: Session):
        self.db = db

    def create_or_update_peer_operational_intent_details(
        self,
        peer_operational_intent_id: str,
        operational_intent_details: OperationalIntentUSSDetails,
    ) -> None | PeerOperationalIntentDetail:
        try:
            _operational_intent_details = asdict(operational_intent_details)

            peer_operational_intent_detail_obj = PeerOperationalIntentDetail(
                id=peer_operational_intent_id,
                volumes=_operational_intent_details["volumes"],
                off_nominal_volumes=_operational_intent_details["off_nominal_volumes"],
                priority=operational_intent_details.priority,
            )
            self.db.add(peer_operational_intent_detail_obj)
            self.db.commit()
            self.db.refresh(peer_operational_intent_detail_obj)
            return peer_operational_intent_detail_obj
        except IntegrityError:
            self.db.rollback()
            return None

    def create_or_update_peer_operational_intent_reference(
        self,
        peer_operational_intent_reference_id: str,
        peer_operational_intent_reference: OperationalIntentReferenceDSSResponse,
    ) -> None | PeerOperationalIntentReference:
        try:
            peer_operational_intent_reference_obj = PeerOperationalIntentReference(
                id=peer_operational_intent_reference_id,
                uss_base_url=peer_operational_intent_reference.uss_base_url,
                ovn=peer_operational_intent_reference.ovn,
                state=peer_operational_intent_reference.state,
                uss_availability=peer_operational_intent_reference.uss_availability,
                version=peer_operational_intent_reference.version,
                time_start=peer_operational_intent_reference.time_start.value,
                time_end=peer_operational_intent_reference.time_end.value,
                subscription_id=peer_operational_intent_reference.subscription_id,
            )
            self.db.add(peer_operational_intent_reference_obj)
            self.db.commit()
            self.db.refresh(peer_operational_intent_reference_obj)
            return peer_operational_intent_reference_obj
        except IntegrityError:
            self.db.rollback()
            return None

    def get_peer_operational_intent_reference_by_id(self, operational_intent_reference_id: str) -> None | PeerOperationalIntentReference:
        return self.db.query(PeerOperationalIntentReference).filter(PeerOperationalIntentReference.id == operational_intent_reference_id).first()

    def write_flight_observation(self, single_observation: SingleAirtrafficObservation) -> bool:
        session_id = single_observation.session_id if single_observation.session_id else "00000000-0000-0000-0000-000000000000"
        try:
            flight_observation = FlightObservation(
                session_id=session_id,
                traffic_source=single_observation.traffic_source,
                latitude_dd=single_observation.lat_dd,
                longitude_dd=single_observation.lon_dd,
                altitude_mm=single_observation.altitude_mm,
                source_type=single_observation.source_type,
                icao_address=single_observation.icao_address,
                metadata=json.dumps(single_observation.metadata),
            )
            self.db.add(flight_observation)
            self.db.commit()
            self.db.refresh(flight_observation)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def delete_flight_declaration(self, flight_declaration_id: str) -> bool:
        try:
            flight_declaration = self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()
            if flight_declaration is None:
                return False
            self.db.delete(flight_declaration)
            self.db.commit()
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_operator_rid_notification(self, operator_rid_notification: OperatorRIDNotificationCreationPayload) -> bool:
        try:
            operator_rid_notification_obj = OperatorRIDNotification(
                message=operator_rid_notification.message,
                session_id=operator_rid_notification.session_id,
            )
            self.db.add(operator_rid_notification_obj)
            self.db.commit()
            self.db.refresh(operator_rid_notification_obj)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_flight_declaration(self, flight_declaration_creation: FlightDeclarationCreationPayload) -> None | FlightDeclaration:
        try:
            flight_declaration = FlightDeclaration(
                id=flight_declaration_creation.id,
                operational_intent=flight_declaration_creation.operational_intent,
                flight_declaration_raw_geojson=flight_declaration_creation.flight_declaration_raw_geojson,
                bounds=flight_declaration_creation.bounds,
                aircraft_id=flight_declaration_creation.aircraft_id,
                state=flight_declaration_creation.state,
            )
            self.db.add(flight_declaration)
            self.db.commit()
            self.db.refresh(flight_declaration)
            return flight_declaration

        except IntegrityError:
            self.db.rollback()
            return None

    def set_flight_declaration_non_conforming(self, flight_declaration: FlightDeclaration):
        flight_declaration.state = 3
        self.db.add(flight_declaration)
        self.db.commit()
        self.db.refresh(flight_declaration)

    def create_flight_operational_intent_reference_with_submitted_operational_intent(
        self,
        flight_declaration: FlightDeclaration,
        operational_intent_reference_payload: (OperationalIntentReferenceDSSResponse | PartialCreateOperationalIntentReference),
    ) -> None | FlightOperationalIntentReference:
        try:
            flight_operational_intent_reference = FlightOperationalIntentReference(
                id=operational_intent_reference_payload.id,  # assigned by the DSS
                declaration=flight_declaration,
                ovn=operational_intent_reference_payload.ovn,
                state=operational_intent_reference_payload.state,
                uss_availability=operational_intent_reference_payload.uss_availability,
                uss_base_url=operational_intent_reference_payload.uss_base_url,
                version=operational_intent_reference_payload.version,
                time_start=operational_intent_reference_payload.time_start.value,
                manager=operational_intent_reference_payload.manager,
                time_end=operational_intent_reference_payload.time_end.value,
                subscription_id=operational_intent_reference_payload.subscription_id,
            )
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)

            return flight_operational_intent_reference

        except IntegrityError:
            self.db.rollback()
            return None

    def create_flight_operational_intent_reference_subscribers(
        self,
        flight_declaration: FlightDeclaration,
        subscribers: list[SubscriberToNotify],
    ) -> bool:
        try:
            flight_operational_intent_reference = (
                self.db.query(FlightOperationalIntentReference)
                .filter(FlightOperationalIntentReference.declaration_id == flight_declaration.id)
                .first()
            )
            if flight_operational_intent_reference is None:
                return False
            else:
                for subscriber in subscribers:
                    all_subscriptions = []
                    for subscrition in subscriber.subscriptions:
                        all_subscriptions.append(asdict(subscrition))

                    subscriber = Subscriber(
                        operational_intent_reference=flight_operational_intent_reference,
                        uss_base_url=subscriber.uss_base_url,
                        subscriptions=json.dumps(all_subscriptions),
                    )
                    self.db.add(subscriber)
                self.db.commit()

            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_flight_operational_intent_details_with_submitted_operational_intent(
        self,
        flight_declaration: FlightDeclaration,
        operational_intent_details_payload: OperationalIntentUSSDetails,
    ) -> None | FlightOperationalIntentDetail:
        try:
            _operational_intent_details_payload = asdict(operational_intent_details_payload)
            flight_operational_intent_detail_obj = FlightOperationalIntentDetail(
                declaration=flight_declaration,
                volumes=json.dumps(_operational_intent_details_payload["volumes"]),
                off_nominal_volumes=json.dumps(_operational_intent_details_payload["off_nominal_volumes"]),
                priority=operational_intent_details_payload.priority,
            )
            self.db.add(flight_operational_intent_detail_obj)
            self.db.commit()
            self.db.refresh(flight_operational_intent_detail_obj)

            return flight_operational_intent_detail_obj

        except IntegrityError:
            self.db.rollback()
            return None

    def write_flight_conformance_record(
        self,
        flight_declaration: FlightDeclaration,
        conformance_non_conformance_state: int,
        description: str,
        event_type: str,
        geofence_breach: bool,
        resolved: bool,
        geofence: Optional[GeoFence],
    ) -> None | ConformanceRecord:
        try:
            conformance_record = ConformanceRecord(
                flight_declaration=flight_declaration,
                conformance_state=conformance_non_conformance_state,
                description=description,
                event_type=event_type,
                geofence_breach=geofence_breach,
                geofence=geofence,
                resolved=resolved,
            )
            self.db.add(conformance_record)
            self.db.commit()
            self.db.refresh(conformance_record)
            return conformance_record
        except IntegrityError:
            self.db.rollback()
            return None

    def create_or_update_peer_composite_operational_intent(
        self,
        operation_id: str,
        composite_operational_intent: CompositeOperationalIntentPayload,
    ) -> bool:
        try:
            peer_operational_intent_details = self.db.query(PeerOperationalIntentDetail).filter(PeerOperationalIntentDetail.id == operation_id).first()
            peer_operational_intent_reference = self.db.query(PeerOperationalIntentReference).filter(PeerOperationalIntentReference.id == operation_id).first()
            if peer_operational_intent_details is None or peer_operational_intent_reference is None:
                return False

            composite_operational_intent_obj = PeerCompositeOperationalIntent(
                start_datetime=composite_operational_intent.start_datetime,
                end_datetime=composite_operational_intent.end_datetime,
                alt_min=composite_operational_intent.alt_min,
                alt_max=composite_operational_intent.alt_max,
                operational_intent_details=peer_operational_intent_details,
                operational_intent_reference=peer_operational_intent_reference,
            )
            self.db.add(composite_operational_intent_obj)
            self.db.commit()
            self.db.refresh(composite_operational_intent_obj)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_or_update_composite_operational_intent(
        self,
        flight_declaration: FlightDeclaration,
        composite_operational_intent_payload: (CompositeOperationalIntentPayload | OperationalIntentStorage),
    ) -> bool:
        try:
            operational_intent_details = (
                self.db.query(FlightOperationalIntentDetail)
                .filter(FlightOperationalIntentDetail.declaration_id == flight_declaration.id)
                .first()
            )
            operational_intent_reference = (
                self.db.query(FlightOperationalIntentReference)
                .filter(FlightOperationalIntentReference.declaration_id == flight_declaration.id)
                .first()
            )

            if operational_intent_reference is None or operational_intent_details is None:
                return False

            composite_operational_intent_obj = CompositeOperationalIntent(
                declaration=flight_declaration,
                bounds=composite_operational_intent_payload.bounds,
                start_datetime=composite_operational_intent_payload.start_datetime,
                end_datetime=composite_operational_intent_payload.end_datetime,
                alt_min=composite_operational_intent_payload.alt_min,
                alt_max=composite_operational_intent_payload.alt_max,
                operational_intent_details=operational_intent_details,
                operational_intent_reference=operational_intent_reference,
            )
            self.db.add(composite_operational_intent_obj)
            self.db.commit()
            self.db.refresh(composite_operational_intent_obj)

            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def update_flight_operational_intent_reference_with_dss_response(
        self,
        flight_declaration: FlightDeclaration,
        dss_operational_intent_reference_id: str,
        ovn: str,
        dss_response: OperationalIntentReferenceDSSResponse,
    ) -> bool:
        try:
            flight_operational_intent_reference = FlightOperationalIntentReference(
                declaration=flight_declaration,
                id=dss_operational_intent_reference_id,
                ovn=ovn,
                dss_response=json.dumps(asdict(dss_response)),
            )
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True

        except IntegrityError:
            self.db.rollback()
            return False

    def create_flight_operational_intent_reference_from_flight_declaration_obj(self, flight_declaration: FlightDeclaration) -> bool:
        try:
            flight_operational_intent_reference = FlightOperationalIntentReference(declaration=flight_declaration)
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True
        except IntegrityError:
            self.db.rollback()
            return False
        except Exception:
            self.db.rollback()
            return False

    def create_flight_operational_intent_reference(
        self,
        flight_declaration: FlightDeclaration,
        created_operational_intent_reference: OperationalIntentReferenceDSSResponse,
    ) -> bool | FlightOperationalIntentReference:
        try:
            flight_operational_intent_reference = FlightOperationalIntentReference(
                id=created_operational_intent_reference.id,
                declaration=flight_declaration,
                uss_availability=created_operational_intent_reference.uss_availability,
                ovn=created_operational_intent_reference.ovn,
                manager=created_operational_intent_reference.manager,
                state=created_operational_intent_reference.state,
                uss_base_url=created_operational_intent_reference.uss_base_url,
                version=created_operational_intent_reference.version,
                time_start=created_operational_intent_reference.time_start.value,
                time_end=created_operational_intent_reference.time_end.value,
                subscription_id=created_operational_intent_reference.subscription_id,
            )
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return flight_operational_intent_reference
        except IntegrityError as ie:
            self.db.rollback()
            logger.error("IntegrityError while creating operational intent reference: %s" % ie)
            return False
        except Exception as e:
            self.db.rollback()
            logger.error("Error while creating operational intent reference: %s" % e)
            return False

    def update_telemetry_timestamp(self, flight_declaration_id: str) -> bool:
        now = arrow.now().isoformat()
        flight_declaration = self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()
        if flight_declaration is None:
            return False
        try:
            flight_declaration.latest_telemetry_datetime = now
            self.db.add(flight_declaration)
            self.db.commit()
            self.db.refresh(flight_declaration)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operational_intent_reference_op_int(
        self,
        flight_operational_intent_reference: FlightOperationalIntentReference,
        dss_operational_intent_reference_id,
    ) -> bool:
        try:
            flight_operational_intent_reference.id = dss_operational_intent_reference_id
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operational_intent_reference_ovn(
        self,
        flight_operational_intent_reference: FlightOperationalIntentReference,
        ovn: str,
    ) -> bool:
        try:
            flight_operational_intent_reference.ovn = ovn
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operational_intent_reference(
        self,
        flight_operational_intent_reference: FlightOperationalIntentReference,
        update_operational_intent_reference: OperationalIntentReferenceDSSResponse,
    ) -> bool:
        try:
            flight_operational_intent_reference.ovn = update_operational_intent_reference.ovn
            flight_operational_intent_reference.state = update_operational_intent_reference.state
            flight_operational_intent_reference.uss_availability = update_operational_intent_reference.uss_availability
            flight_operational_intent_reference.uss_base_url = update_operational_intent_reference.uss_base_url
            flight_operational_intent_reference.version = update_operational_intent_reference.version
            flight_operational_intent_reference.time_start = update_operational_intent_reference.time_start.value
            flight_operational_intent_reference.time_end = update_operational_intent_reference.time_end.value
            flight_operational_intent_reference.subscription_id = update_operational_intent_reference.subscription_id
            flight_operational_intent_reference.manager = update_operational_intent_reference.manager

            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operational_intent_details(
        self,
        flight_operational_intent_detail: FlightOperationalIntentDetail,
        operational_intent_details: OperationalIntentUSSDetails,
    ) -> bool:
        _volumes = []
        _off_nominal_volumes = operational_intent_details.off_nominal_volumes
        for volume in operational_intent_details.volumes:
            _volumes.append(asdict(volume))
        _off_nominal_volumes = []
        for volume in operational_intent_details.off_nominal_volumes:
            _off_nominal_volumes.append(asdict(volume))

        try:
            flight_operational_intent_detail.volumes = json.dumps(_volumes)
            flight_operational_intent_detail.off_nominal_volumes = json.dumps(_off_nominal_volumes)
            flight_operational_intent_detail.priority = operational_intent_details.priority
            self.db.add(flight_operational_intent_detail)
            self.db.commit()
            self.db.refresh(flight_operational_intent_detail)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operational_intent_reference_op_int_ovn(
        self,
        flight_operational_intent_reference: FlightOperationalIntentReference,
        dss_operational_intent_reference_id: str,
        ovn: str,
    ) -> bool:
        try:
            flight_operational_intent_reference.id = dss_operational_intent_reference_id
            flight_operational_intent_reference.ovn = ovn
            self.db.add(flight_operational_intent_reference)
            self.db.commit()
            self.db.refresh(flight_operational_intent_reference)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operation_operational_intent(
        self,
        flight_declaration_id: str,
        operational_intent: PartialCreateOperationalIntentReference,
    ) -> bool:
        try:
            flight_declaration = self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()
            if flight_declaration is None:
                return False
            flight_declaration.operational_intent = json.dumps(asdict(operational_intent))
            # TODO: Convert the updated operational intent to GeoJSON
            self.db.add(flight_declaration)
            self.db.commit()
            self.db.refresh(flight_declaration)
            return True
        except Exception:
            self.db.rollback()
            return False

    def update_flight_operation_state(self, flight_declaration_id: str, state: int) -> bool:
        try:
            flight_declaration = self.db.query(FlightDeclaration).filter(FlightDeclaration.id == flight_declaration_id).first()
            if flight_declaration is None:
                return False
            flight_declaration.state = state
            self.db.add(flight_declaration)
            self.db.commit()
            self.db.refresh(flight_declaration)
            return True
        except Exception:
            self.db.rollback()
            return False

    def create_surveillance_session(self, session_id: str, valid_until: str) -> bool:
        try:
            surveillance_session = SurveillanceSession(
                id=session_id,
                valid_until=valid_until,
            )
            self.db.add(surveillance_session)
            self.db.commit()
            self.db.refresh(surveillance_session)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_surveillance_monitoring_heartbeat_periodic_task(self, session_id: str) -> bool:
        now = arrow.now()
        session_id = session_id if session_id else str(uuid.uuid4())

        expires = now.shift(minutes=1)
        task_name = "send_heartbeat_to_consumer"
        logger.info("Creating periodic task for surveillance monitoring, it expires at %s" % expires)
        try:
            task_scheduler = TaskScheduler(
                periodic_task_name=task_name,
                session_id=session_id,
            )
            self.db.add(task_scheduler)
            self.db.commit()
            self.db.refresh(task_scheduler)
            logger.warning("Periodic task '%s' registered in DB. Use a separate Celery beat scheduler to dispatch it." % task_name)
            return True
        except Exception as e:
            self.db.rollback()
            logger.debug(f"{e}")
            logger.error("Could not create surveillance monitoring heartbeat periodic task")
            return False

    def create_surveillance_monitoring_track_periodic_task(self, session_id: str) -> bool:
        now = arrow.now()
        session_id = session_id if session_id else str(uuid.uuid4())

        expires = now.shift(minutes=1)
        task_name = "send_and_generate_track_to_consumer"
        logger.info("Creating periodic task for surveillance monitoring tracks, it expires at %s" % expires)
        try:
            task_scheduler = TaskScheduler(
                periodic_task_name=task_name,
                session_id=session_id,
            )
            self.db.add(task_scheduler)
            self.db.commit()
            self.db.refresh(task_scheduler)
            logger.warning("Periodic task '%s' registered in DB. Use a separate Celery beat scheduler to dispatch it." % task_name)
            return True
        except Exception as e:
            self.db.rollback()
            logger.debug(f"Error creating surveillance monitoring heartbeat periodic task: {e}")
            logger.error("Could not create surveillance monitoring heartbeat periodic task")
            return False

    def remove_track_monitoring_heartbeat_periodic_task(self, track_monitoring_heartbeat_task: TaskScheduler):
        self.db.delete(track_monitoring_heartbeat_task)
        self.db.commit()

    def remove_surveillance_monitoring_heartbeat_periodic_task(self, surveillance_monitoring_heartbeat_task: TaskScheduler):
        self.db.delete(surveillance_monitoring_heartbeat_task)
        self.db.commit()

    def create_conformance_monitoring_periodic_task(self, flight_declaration: FlightDeclaration) -> bool:
        every = int(os.getenv("HEARTBEAT_RATE_SECS", default=5))
        now = arrow.now()
        session_id = str(uuid.uuid4())
        fd_end = arrow.get(flight_declaration.end_datetime)
        delta = fd_end - now
        delta_seconds = delta.total_seconds()
        expires = now.shift(seconds=delta_seconds)
        task_name = "check_flight_conformance"
        logger.info("Creating periodic task for conformance monitoring expires at %s" % expires)
        try:
            task_scheduler = TaskScheduler(
                periodic_task_name=task_name,
                session_id=session_id,
                flight_declaration=flight_declaration,
            )
            self.db.add(task_scheduler)
            self.db.commit()
            self.db.refresh(task_scheduler)
            logger.warning("Periodic task '%s' registered in DB. Use a separate Celery beat scheduler to dispatch it." % task_name)
            return True
        except Exception as e:
            self.db.rollback()
            logger.debug(f"Error creating conformance monitoring periodic task: {e}")
            logger.error("Could not create periodic task")
            return False

    def remove_conformance_monitoring_periodic_task(self, conformance_monitoring_task: TaskScheduler):
        self.db.delete(conformance_monitoring_task)
        self.db.commit()

    def create_rid_stream_monitoring_periodic_task(self, session_id: str, end_datetime: str) -> bool:
        every = int(os.getenv("HEARTBEAT_RATE_SECS", default=5))
        now = arrow.now()
        stream_end = arrow.get(end_datetime)
        delta = stream_end - now
        delta_seconds = delta.total_seconds()
        expires = now.shift(seconds=delta_seconds)
        task_name = "check_rid_stream_conformance"

        try:
            task_scheduler = TaskScheduler(
                periodic_task_name=task_name,
                session_id=session_id,
            )
            self.db.add(task_scheduler)
            self.db.commit()
            self.db.refresh(task_scheduler)
            logger.warning("Periodic task '%s' registered in DB. Use a separate Celery beat scheduler to dispatch it." % task_name)
            logger.error("Created and starting RID stream observation task")
            return True
        except Exception as e:
            self.db.rollback()
            logger.error("Could not create RID stream observation periodic task %s" % e)
            return False

    def remove_rid_stream_monitoring_periodic_task(self, rid_stream_monitoring_task: TaskScheduler):
        self.db.delete(rid_stream_monitoring_task)
        self.db.commit()

    def create_rid_subscription_record(
        self,
        subscription_id: str,
        record_id: str,
        view: str,
        view_hash: int,
        end_datetime: str,
        flights_dict: str,
        is_simulated: bool,
    ) -> bool:
        try:
            rid_subscription = ISASubscription(
                id=record_id,
                subscription_id=subscription_id,
                view=view,
                view_hash=view_hash,
                end_datetime=end_datetime,
                flight_details=flights_dict,
                is_simulated=is_simulated,
            )
            self.db.add(rid_subscription)
            self.db.commit()
            self.db.refresh(rid_subscription)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def update_flight_details_in_rid_subscription_record(self, existing_subscription_record: ISASubscription, flights_dict: str) -> bool:
        try:
            existing_subscription_record.flight_details = flights_dict
            self.db.add(existing_subscription_record)
            self.db.commit()
            self.db.refresh(existing_subscription_record)
            return True
        except Exception:
            self.db.rollback()
            return False

    def delete_all_simulated_rid_subscription_records(self) -> bool:
        try:
            self.db.query(ISASubscription).filter(ISASubscription.is_simulated == True).delete()  # noqa: E712
            self.db.commit()
            return True
        except Exception:
            self.db.rollback()
            return False

    def clear_stored_operational_intents(self):
        self.db.query(PeerOperationalIntentReference).filter(PeerOperationalIntentReference.is_live == False).delete()  # noqa: E712
        self.db.query(PeerOperationalIntentDetail).filter(PeerOperationalIntentDetail.is_live == False).delete()  # noqa: E712
        self.db.commit()

    def write_constraint_details(self, constraint_id: str, constraint: ConstraintData) -> bool:
        try:
            constraint_obj = ConstraintDetail(
                id=constraint_id,
                details=json.dumps(asdict(constraint.details)),
            )
            self.db.add(constraint_obj)
            self.db.commit()
            self.db.refresh(constraint_obj)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def write_constraint_reference_details(self, constraint: ConstraintData) -> bool:
        try:
            constraint_reference_obj = ConstraintReference(
                id=constraint.reference.id,
                ovn=constraint.reference.ovn,
                details=json.dumps(asdict(constraint.reference)),
            )
            self.db.add(constraint_reference_obj)
            self.db.commit()
            self.db.refresh(constraint_reference_obj)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def create_or_update_composite_constraint(self, composite_constraint_payload: CompositeConstraintPayload) -> CompositeConstraint | bool:
        try:
            composite_constraint_obj = CompositeConstraint(
                constraint_reference_id=composite_constraint_payload.constraint_reference_id,
                constraint_detail_id=composite_constraint_payload.constraint_detail_id,
                declaration_id=composite_constraint_payload.flight_declaration_id,
                bounds=composite_constraint_payload.bounds,
                start_datetime=composite_constraint_payload.start_datetime,
                end_datetime=composite_constraint_payload.start_datetime,
                alt_max=composite_constraint_payload.alt_max,
                alt_min=composite_constraint_payload.alt_min,
            )
            self.db.add(composite_constraint_obj)
            self.db.commit()
            self.db.refresh(composite_constraint_obj)
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def update_constraint_reference_ovn(
        self,
        constraint_reference: ConstraintReference,
        ovn: str,
    ) -> bool:
        try:
            constraint_reference.ovn = ovn
            self.db.add(constraint_reference)
            self.db.commit()
            self.db.refresh(constraint_reference)
            return True

        except IntegrityError:
            self.db.rollback()
            return False

    def create_or_update_geofence(self, geofence_payload: GeofencePayload) -> None | GeoFence:
        try:
            geofence = GeoFence(
                raw_geo_fence=json.dumps(geofence_payload.raw_geo_fence),
                id=geofence_payload.id,
                upper_limit=geofence_payload.upper_limit,
                lower_limit=geofence_payload.lower_limit,
                altitude_ref=geofence_payload.altitude_ref,
                bounds=geofence_payload.bounds,
                status=geofence_payload.status,
                message=geofence_payload.message,
                is_test_dataset=geofence_payload.is_test_dataset,
                start_datetime=geofence_payload.start_datetime.value,
                end_datetime=geofence_payload.end_datetime.value,
                geozone=json.dumps(geofence_payload.geozone, cls=EnhancedJSONEncoder),
            )
            self.db.add(geofence)
            self.db.commit()
            self.db.refresh(geofence)
            return geofence
        except IntegrityError:
            self.db.rollback()
            return None

    def create_or_update_constraint_detail(self, constraint: ConstraintDetails, geofence: GeoFence) -> ConstraintDetail | None:
        try:
            _constraint_volumes = []
            for _volume in constraint.volumes:
                _constraint_volumes.append(asdict(_volume))
            constraint_obj = ConstraintDetail(
                volumes=json.dumps(_constraint_volumes),
                _type=constraint.type,
                geofence=geofence,
            )
            self.db.add(constraint_obj)
            self.db.commit()
            self.db.refresh(constraint_obj)
            return constraint_obj
        except IntegrityError:
            self.db.rollback()
            return None

    def create_or_update_constraint_reference(
        self,
        constraint_reference: ConstraintReferencePayload,
        geofence: GeoFence,
        flight_declaration: FlightDeclaration,
    ) -> ConstraintReference | None:
        try:
            constraint_obj = ConstraintReference(
                id=constraint_reference.id,
                ovn=constraint_reference.ovn,
                uss_base_url=constraint_reference.uss_base_url,
                uss_availability=constraint_reference.uss_availability,
                version=constraint_reference.version,
                time_start=constraint_reference.time_start.value,
                time_end=constraint_reference.time_end.value,
                geofence=geofence,
                flight_declaration=flight_declaration,
            )
            self.db.add(constraint_obj)
            self.db.commit()
            self.db.refresh(constraint_obj)
            return constraint_obj
        except IntegrityError:
            self.db.rollback()
            return None

    def _serialize_operator_location(self, operator_location):
        return json.dumps(asdict(operator_location)) if operator_location else json.dumps({})

    def _serialize_auth_data(self, auth_data):
        return json.dumps(asdict(auth_data)) if auth_data else json.dumps({})

    def _serialize_eu_classification(self, eu_classification):
        return json.dumps(asdict(eu_classification)) if eu_classification else json.dumps({})

    def _serialize_uas_id(self, uas_id):
        return json.dumps(asdict(uas_id)) if uas_id else json.dumps({})

    def _create_rid_flight_details(self, rid_flight_details_payload: RIDFlightDetails):
        operator_location = self._serialize_operator_location(rid_flight_details_payload.operator_location)
        auth_data = self._serialize_auth_data(rid_flight_details_payload.auth_data)
        eu_classification = self._serialize_eu_classification(rid_flight_details_payload.eu_classification)
        uas_id = self._serialize_uas_id(rid_flight_details_payload.uas_id)
        try:
            rid_flight_details = RIDFlightDetail(
                id=rid_flight_details_payload.id,
                operation_description=rid_flight_details_payload.operation_description,
                operator_location=operator_location,
                operator_id=rid_flight_details_payload.operator_id,
                auth_data=auth_data,
                uas_id=uas_id,
                eu_classification=eu_classification,
            )
            self.db.add(rid_flight_details)
            self.db.commit()
            self.db.refresh(rid_flight_details)
            return rid_flight_details
        except IntegrityError:
            self.db.rollback()
            return None

    def create_or_update_rid_flight_details(self, rid_flight_details_payload: RIDFlightDetails):
        rid_flight_details = self.db.query(RIDFlightDetail).filter(RIDFlightDetail.id == rid_flight_details_payload.id).first()
        operator_location = self._serialize_operator_location(rid_flight_details_payload.operator_location)
        auth_data = self._serialize_auth_data(rid_flight_details_payload.auth_data)
        eu_classification = self._serialize_eu_classification(rid_flight_details_payload.eu_classification)
        uas_id = self._serialize_uas_id(rid_flight_details_payload.uas_id)
        if rid_flight_details is not None:
            rid_flight_details.operation_description = rid_flight_details_payload.operation_description
            rid_flight_details.operator_location = operator_location
            rid_flight_details.auth_data = auth_data
            rid_flight_details.uas_id = uas_id
            rid_flight_details.eu_classification = eu_classification
            try:
                self.db.add(rid_flight_details)
                self.db.commit()
                self.db.refresh(rid_flight_details)
                return rid_flight_details
            except IntegrityError:
                self.db.rollback()
                return None
        else:
            return self._create_rid_flight_details(rid_flight_details_payload)

    def delete_all_flight_observations(self) -> bool:
        try:
            self.db.query(FlightObservation).delete()
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting all flight observations: {e}")
            return False

    def delete_all_flight_details(self) -> bool:
        try:
            self.db.query(RIDFlightDetail).delete()
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting all flight observations: {e}")
            return False

    def update_sensor_health_status(self, sensor_id: str, new_status: str, recovery_type: Optional[str] = None) -> bool:
        """
        Canonical method for all sensor health changes.
        1. Updates SurveillanceSensorHealth.status
        2. Creates SurveillanceSensortHealthTracking record (with recovery_type)
        3. Calls process_sensor_status_change handler for any status transition

        recovery_type should be "automatic" or "manual" when new_status == "operational"
        and the sensor is recovering from degraded/outage. Pass None for failure transitions.
        """
        from surveillance_monitoring_operations.custom_signals import process_sensor_status_change

        sensor = self.db.query(SurveillanceSensor).filter(SurveillanceSensor.id == sensor_id).first()
        if sensor is None:
            logger.error(f"update_sensor_health_status: sensor {sensor_id} not found")
            return False

        health = self.db.query(SurveillanceSensorHealth).filter(SurveillanceSensorHealth.sensor_id == sensor.id).first()
        created = health is None
        if created:
            health = SurveillanceSensorHealth(sensor_id=sensor.id, status=new_status)
            self.db.add(health)
            self.db.commit()
            self.db.refresh(health)

        previous_status = health.status if not created else new_status

        if not created:
            if previous_status == new_status:
                return True
            health.status = new_status
            self.db.add(health)
            self.db.commit()
            self.db.refresh(health)

        tracking = SurveillanceSensortHealthTracking(
            sensor_id=sensor.id,
            status=new_status,
            recovery_type=recovery_type,
        )
        self.db.add(tracking)
        self.db.commit()

        process_sensor_status_change(
            sender="update_sensor_health_status",
            sensor_id=sensor_id,
            previous_status=previous_status,
            new_status=new_status,
            recovery_type=recovery_type,
        )
        return True

    def record_heartbeat_event(self, session_id: str, expected_at: datetime, delivered_on_time: bool) -> bool:
        try:
            session = self.db.query(SurveillanceSession).filter(SurveillanceSession.id == session_id).first()
            if session is None:
                logger.error(f"record_heartbeat_event: session {session_id} not found")
                return False
            heartbeat_event = SurveillanceHeartbeatEvent(
                session=session,
                expected_at=expected_at,
                delivered_on_time=delivered_on_time,
            )
            self.db.add(heartbeat_event)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"record_heartbeat_event: {e}")
            return False

    def record_track_event(self, session_id: str, expected_at: datetime, had_active_tracks: bool) -> bool:
        try:
            session = self.db.query(SurveillanceSession).filter(SurveillanceSession.id == session_id).first()
            if session is None:
                logger.error(f"record_track_event: session {session_id} not found")
                return False
            track_event = SurveillanceTrackEvent(
                session=session,
                expected_at=expected_at,
                had_active_tracks=had_active_tracks,
            )
            self.db.add(track_event)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"record_track_event: {e}")
            return False
