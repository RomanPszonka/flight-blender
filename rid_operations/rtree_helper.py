from rtree import index
from typing import List
from os import environ as env
import redis
import json 

class OperationalIntentsIndexFactory():
    def __init__(self, index_name:str):
        self.idx = index.Index(index_name)
        self.r = redis.Redis(host=env.get('REDIS_HOST',"redis"), port =env.get('REDIS_PORT',6379)) 

    def add_box_to_index(self,enumerated_id:int,  flight_id:str, view:List[float], start_time:str, end_time:str):        
        metadata = {"start_time":start_time, "end_time":end_time, "flight_id":flight_id }
        self.idx.insert(id = enumerated_id, coordinates= (view[0], view[1], view[2], view[3]),obj = metadata)

    def delete_from_index(self,enumerated_id:int, view:List[float]):                
        self.idx.delete(id = enumerated_id, coordinates= (view[0], view[1], view[2], view[3]))

    def generate_operational_intents_index(self) -> None:
        """This method generates a rTree index of currently active operational indexes """      
        
        all_op_ints = self.r.keys(pattern='opint.*')
        for flight_idx, flight_id in enumerate(all_op_ints):                        
            flight_id_str = flight_id.decode().split('.')[1]        
            operational_intent_view_raw = self.r.get(flight_id)   
            operational_intent_view = json.loads(operational_intent_view_raw)            
            split_view = operational_intent_view['bounds'].split(",")
            start_time = operational_intent_view['start_time']
            end_time  = operational_intent_view['end_time']            
            view = [float(i) for i in split_view]            
            self.add_box_to_index(enumerated_id= flight_idx, flight_id = flight_id_str, view = view, start_time=start_time, end_time= end_time)

    def clear_rtree_index(self):
        """Method to delete all boxes from the index"""

        all_op_ints = self.r.keys(pattern='opint.*')
        for flight_idx, flight_id in enumerate(all_op_ints):           
            operational_intent_view_raw = self.r.get(flight_id)   
            operational_intent_view = json.loads(operational_intent_view_raw)            
            split_view = operational_intent_view['bounds'].split(",")
            view = [float(i) for i in split_view]            
            self.delete_from_index(enumerated_id= flight_idx,view = view)

    def check_box_intersection(self, view_box:List[float]):
        intersections = [n.object for n in self.idx.intersection((view_box[0], view_box[1], view_box[2], view_box[3]), objects=True)]        
        return intersections