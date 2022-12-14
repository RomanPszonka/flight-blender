# Create your views here.
import uuid
from auth_helper.utils import requires_scopes
# Create your views here.
import json
import arrow
from typing import List
from . import rtree_geo_fence_helper
from rest_framework.decorators import api_view
from shapely.geometry import shape, Point
from .models import GeoFence
from django.http import HttpResponse
from .tasks import write_geo_fence, write_geo_zone, download_geozone_source
from shapely.ops import unary_union
from rest_framework import mixins, generics
from .serializers import GeoFenceSerializer
from django.utils.decorators import method_decorator
from decimal import Decimal
from .data_definitions import GeoAwarenessTestHarnessStatus, GeoAwarenessTestStatus, GeoZoneHttpsSource, GeoZoneCheckRequestBody, GeoZoneChecksResponse, GeoZoneCheckResult, GeoZoneFilterPosition
from django.http import JsonResponse
import logging
import pyproj
from buffer_helper import toFromUTM, convert_shapely_to_geojson
from auth_helper.common import get_redis
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from dataclasses import asdict, is_dataclass
from .common import validate_geo_zone
from .geofence_typing import ImplicitDict
logger = logging.getLogger('django')


class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if is_dataclass(o):
                return asdict(o)
            return super().default(o)


INDEX_NAME = 'geofence_proc'

@api_view(['POST'])
@requires_scopes(['blender.write'])
def set_geo_fence(request):  
    
    try:
        assert request.headers['Content-Type'] == 'application/json'   
    except AssertionError as ae:     
        msg = {"message":"Unsupported Media Type"}
        return HttpResponse(json.dumps(msg), status=415, mimetype='application/json')

    try:         
        geo_json_fc = request.data
    except KeyError as ke: 
        msg = json.dumps({"message":"A geofence object is necessary in the body of the request"})        
        return HttpResponse(msg, status=400)    

    shp_features = []
    for feature in geo_json_fc['features']:
        shp_features.append(shape(feature['geometry']))
    combined_features = unary_union(shp_features)
    bnd_tuple = combined_features.bounds
    bounds = ''.join(['{:.7f}'.format(x) for x in bnd_tuple])
    try:
        s_time = geo_json_fc[0]['properties']["start_time"]
    except KeyError as ke: 
        start_time = arrow.now().isoformat()
    else:
        start_time = arrow.get(s_time).isoformat()
    
    try:
        e_time = geo_json_fc[0]['properties']["end_time"]
    except KeyError as ke:
        end_time = arrow.now().shift(hours=1).isoformat()
    else:            
        end_time = arrow.get(e_time).isoformat()

    try:
        upper_limit = Decimal(geo_json_fc[0]['properties']["upper_limit"])
    except KeyError as ke: 
        upper_limit = 500.00
    
    try:
        lower_limit = Decimal(geo_json_fc[0]['properties']["upper_limit"])
    except KeyError as ke:
        lower_limit = 100.00
             
    try:
        name = geo_json_fc[0]['properties']["name"]
    except KeyError as ke:
        name = "Standard Geofence"
    raw_geo_fence = json.dumps(geo_json_fc)
    geo_f = GeoFence(raw_geo_fence = raw_geo_fence,start_datetime = start_time, end_datetime = end_time, upper_limit= upper_limit, lower_limit=lower_limit, bounds= bounds, name= name)
    geo_f.save()

    write_geo_fence.delay(geo_fence = raw_geo_fence)
    

    op = json.dumps ({"message":"Geofence Declaration submitted", 'id':str(geo_f.id)})
    return HttpResponse(op, status=200)

@api_view(['POST'])
@requires_scopes(['blender.write'])
def set_geozone(request):  
    try:
        assert request.headers['Content-Type'] == 'application/json'   
    except AssertionError as ae:     
        msg = {"message":"Unsupported Media Type"}
        return HttpResponse(json.dumps(msg), status=415, mimetype='application/json')

    try:         
        geo_zone = request.data
    except KeyError as ke: 
        msg = json.dumps({"message":"A geozone object is necessary in the body of the request"})        
        return HttpResponse(msg, status=400)    
    
    is_geo_zone_valid = validate_geo_zone(geo_zone)

    if is_geo_zone_valid:
        write_geo_zone.delay(geo_zone = json.dumps(geo_zone))
        
        geo_f = uuid.uuid4()
        op = json.dumps ({"message":"GeoZone Declaration submitted", 'id':str(geo_f)})
        return HttpResponse(op, status=200)

    else: 
        msg = json.dumps({"message":"A valid geozone object with a description is necessary the body of the request"})        
        return HttpResponse(msg, status=400)   



@method_decorator(requires_scopes(['blender.read']), name='dispatch')
class GeoFenceDetail(mixins.RetrieveModelMixin, 
                    generics.GenericAPIView):

    queryset = GeoFence.objects.filter(is_test_dataset = False)
    serializer_class = GeoFenceSerializer

    def get(self, request, *args, **kwargs):
        return self.retrieve(request, *args, **kwargs)

@method_decorator(requires_scopes(['blender.read']), name='dispatch')
class GeoFenceList(mixins.ListModelMixin,  
    generics.GenericAPIView):

    queryset = GeoFence.objects.filter(is_test_dataset = False)
    serializer_class = GeoFenceSerializer

    def get_relevant_geo_fence(self,start_date, end_date,  view_port:List[float]):               
        present = arrow.now()
        if start_date and end_date:
            s_date = arrow.get(start_date, "YYYY-MM-DD")
            e_date = arrow.get(end_date, "YYYY-MM-DD")
    
        else:             
            s_date = present.shift(days=-1)
            e_date = present.shift(days=1)

        all_fences_within_timelimits = GeoFence.objects.filter(start_datetime__lte = s_date.isoformat(), end_datetime__gte = e_date.isoformat())
        logging.info("Found %s geofences" % len(all_fences_within_timelimits))
        if view_port:
            
            my_rtree_helper = rtree_geo_fence_helper.GeoFenceRTreeIndexFactory()  
            my_rtree_helper.generate_geo_fence_index(all_fences = all_fences_within_timelimits)
            all_relevant_fences = my_rtree_helper.check_box_intersection(view_box = view_port)
            relevant_id_set = []
            for i in all_relevant_fences:
                relevant_id_set.append(i['geo_fence_id'])

            my_rtree_helper.clear_rtree_index()
            filtered_relevant_fences = GeoFence.objects.filter(id__in = relevant_id_set)
            
        else: 
            filtered_relevant_fences = all_fences_within_timelimits

        return filtered_relevant_fences

    def get_queryset(self):
        start_date = self.request.query_params.get('start_date', None)
        end_date = self.request.query_params.get('end_date', None)

        view = self.request.query_params.get('view', None)
        view_port = []
        if view:
            view_port = [float(i) for i in view.split(",")]
        
        responses = self.get_relevant_geo_fence(view_port= view_port,start_date= start_date, end_date= end_date)
        return responses

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

        

@method_decorator(requires_scopes(['geo-awareness.test']), name='dispatch')
class GeoZoneTestHarnessStatus(generics.GenericAPIView):    
    
    def get(self, request, *args, **kwargs):
        status = GeoAwarenessTestHarnessStatus(status="Ready", version="latest")
        return JsonResponse(json.loads(json.dumps(status, cls=EnhancedJSONEncoder)), status=200)


@method_decorator(requires_scopes(['geo-awareness.test']), name='dispatch')
class GeoZoneSourcesOperations(generics.GenericAPIView):    
    def put(self, request, geozone_source_id):
        r = get_redis()                
        try:
            geo_zone_url_details = ImplicitDict.parse(request.data, GeoZoneHttpsSource)
        except KeyError as ke:
            ga_import_response = GeoAwarenessTestStatus(result = 'Rejected', message ="There was an error in processing the request payload, a url and format key is required for successful processing")
            return JsonResponse(json.loads(json.dumps(ga_import_response, cls=EnhancedJSONEncoder)), status=200)

        url_validator = URLValidator()
        try:
            url_validator(geo_zone_url_details.https_source.url)
        except ValidationError as ve:
            ga_import_response = GeoAwarenessTestStatus(result = 'Unsupported', message ="There was an error in the url provided")
            return JsonResponse(json.loads(json.dumps(ga_import_response, cls=EnhancedJSONEncoder)), status=200)

        geoawareness_test_data_store = 'geoawarenes_test.' + str(geozone_source_id)
        result = 'Activating'
        ga_import_response = GeoAwarenessTestStatus(result = result, message="")
        download_geozone_source.delay(geo_zone_url = geo_zone_url_details.https_source.url,geozone_source_id = geozone_source_id)

        r.set(geoawareness_test_data_store, json.dumps(asdict(ga_import_response)))
        r.expire(name = geoawareness_test_data_store, time = 3000)

        return JsonResponse(json.loads(json.dumps(ga_import_response, cls=EnhancedJSONEncoder)), status=200)
                  

    def get(self, request, geozone_source_id):
        geoawareness_test_data_store = 'geoawarenes_test.' + str(geozone_source_id)
        r = get_redis()  
        
        if r.exists(geoawareness_test_data_store):
            test_data_status = r.get(geoawareness_test_data_store)
            test_status = json.loads(test_data_status)
            ga_test_status = GeoAwarenessTestStatus(result=test_status['result'], message="") 
            return JsonResponse(json.loads(json.dumps(ga_test_status, cls=EnhancedJSONEncoder)), status=200)
        else: 
            return JsonResponse({}, status=404)

    def delete(self, request, geozone_source_id):
        geoawareness_test_data_store = 'geoawarenes_test.' + str(geozone_source_id)
        r = get_redis()  
        if r.exists(geoawareness_test_data_store):
            # TODO: delete the test and dataset
            all_test_geozones = GeoFence.objects.filter(is_test_dataset = 1)
            for geozone in all_test_geozones.all(): 
                geozone.delete()
            deletion_status = GeoAwarenessTestStatus(result='Deactivating', message="Test data has been scheduled to be deleted")
            r.set(geoawareness_test_data_store, json.dumps(asdict(deletion_status)))
            return JsonResponse(json.loads(json.dumps(deletion_status, cls=EnhancedJSONEncoder)), status=200)

        else:
            return JsonResponse({}, status=404)

@method_decorator(requires_scopes(['geo-awareness.test']), name='dispatch')
class GeoZoneCheck(generics.GenericAPIView):

    def post(self, request, *args, **kwargs):

        proj = pyproj.Proj(
            proj="utm",
            zone='54N', # UTM Zone for Switzerland
            ellps="WGS84",
            datum="WGS84"
        )

        geo_zone_checks = ImplicitDict.parse(request.data, GeoZoneCheckRequestBody) 
        geo_zones_of_interest = False
        for filter_set in geo_zone_checks.checks.filterSets:
            print(json.dumps(filter_set))

            if 'position' in filter_set:
                filter_position = ImplicitDict.parse(filter_set['position'], GeoZoneFilterPosition)
                relevant_geo_fences = GeoFence.objects.filter(is_test_dataset = 1)
                my_rtree_helper = rtree_geo_fence_helper.GeoFenceRTreeIndexFactory() 
                # Buffer the point to get a small view port / bounds 
                init_point = Point(filter_position)
                init_shape_utm = toFromUTM(init_point, proj)
                buffer_shape_utm = init_shape_utm.buffer(1)
                buffer_shape_lonlat = toFromUTM(buffer_shape_utm, proj, inv=True)
                view_port = buffer_shape_lonlat.bounds

                my_rtree_helper.generate_geo_fence_index(all_fences = relevant_geo_fences)
                all_relevant_fences = my_rtree_helper.check_box_intersection(view_box = view_port)
                my_rtree_helper.clear_rtree_index() 
                if all_relevant_fences:
                    geo_zones_of_interest = True
                
            if 'after' in filter_set:
                after_query = arrow.get(filter_set['after'])
                geo_zones_exist = GeoFence.objects.filter(start_datetime__gte =  after_query ).exists()
                if geo_zones_exist:
                    geo_zones_of_interest = True
            if 'before' in filter_set:
                before_query = arrow.get(filter_set['before'])
                geo_zones_exist = GeoFence.objects.filter(before_datetime__lte =  before_query ).exists()
                if geo_zones_exist:
                    geo_zones_of_interest = True
            if 'ed269' in filter_set: 
                pass
        
        if geo_zones_of_interest: 
            geo_zone_check_result = GeoZoneCheckResult(geozone='Present')
        else: 
            geo_zone_check_result = GeoZoneCheckResult(geozone='Absent')
            

        geo_zone_response = GeoZoneChecksResponse(applicableGeozone =geo_zone_check_result)
        return JsonResponse(json.loads(json.dumps(geo_zone_response, cls=EnhancedJSONEncoder)), status=200)
