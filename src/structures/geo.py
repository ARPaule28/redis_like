import math
from typing import Dict, List, Optional, Tuple
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError, GeoError

class GeoOps:
    EARTH_RADIUS_KM = 6371.0
    
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def geoadd(self, key: str, longitude: float, latitude: float, member: str) -> int:
        if not (-180 <= longitude <= 180):
            raise GeoError("Invalid longitude")
        if not (-90 <= latitude <= 90):
            raise GeoError("Invalid latitude")
        
        if key not in self._data._geo:
            self._data._geo[key] = {}
            self._data._type_map[key] = 'geo'
        
        is_new = member not in self._data._geo[key]
        self._data._geo[key][member] = (longitude, latitude)
        return 1 if is_new else 0
    
    def geodist(self, key: str, member1: str, member2: str, unit: str = 'km') -> Optional[float]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'geo':
            raise WrongTypeError('geo', self._data.type(key))
        
        if member1 not in self._data._geo[key] or member2 not in self._data._geo[key]:
            return None
        
        lon1, lat1 = self._data._geo[key][member1]
        lon2, lat2 = self._data._geo[key][member2]
        
        # Haversine formula
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2 + 
             math.cos(math.radians(lat1)) * 
             math.cos(math.radians(lat2)) * 
             math.sin(dlon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = self.EARTH_RADIUS_KM * c
        
        if unit == 'm':
            return distance * 1000
        elif unit == 'mi':
            return distance * 0.621371
        elif unit == 'ft':
            return distance * 3280.84
        return distance
    
    def georadius(self, key: str, longitude: float, latitude: float, 
                 radius: float, unit: str = 'km') -> List[str]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'geo':
            raise WrongTypeError('geo', self._data.type(key))
        
        results = []
        for member, (lon, lat) in self._data._geo[key].items():
            dist = self.geodist(key, member, "__temp__", unit)
            if dist is not None and dist <= radius:
                results.append(member)
        return results