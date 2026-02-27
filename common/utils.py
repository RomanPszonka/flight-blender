import datetime
import json
from dataclasses import asdict, is_dataclass


class LazyEncoder(json.JSONEncoder):
    def default(self, obj):
        return str(obj)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


class EnhancedJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        for key, value in obj.items():
            try:
                # Attempt to parse datetime strings back into datetime objects
                obj[key] = datetime.datetime.fromisoformat(value)
            except (ValueError, TypeError):
                pass
        return obj
