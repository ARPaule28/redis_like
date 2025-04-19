import threading
from collections import defaultdict
from typing import Dict, Any, Optional
from .exceptions import KeyNotFoundError, WrongTypeError

class DataStore:
    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._expirations: Dict[str, float] = {}
        self._type_map: Dict[str, str] = {}
        self._locks = defaultdict(threading.Lock)
        
    def set(self, key: str, value: Any, data_type: str) -> None:
        with self._locks[key]:
            self._store[key] = value
            self._type_map[key] = data_type
            
    def get(self, key: str) -> Any:
        if key not in self._store:
            raise KeyNotFoundError(key)
        return self._store[key]
    
    def delete(self, key: str) -> bool:
        with self._locks[key]:
            if key in self._store:
                del self._store[key]
                if key in self._expirations:
                    del self._expirations[key]
                del self._type_map[key]
                return True
            return False
    
    def type_of(self, key: str) -> Optional[str]:
        return self._type_map.get(key)
    
    def expire(self, key: str, seconds: float) -> bool:
        if key not in self._store:
            return False
        self._expirations[key] = time.time() + seconds
        return True
    
    def ttl(self, key: str) -> Optional[float]:
        if key not in self._expirations:
            return None
        remaining = self._expirations[key] - time.time()
        return remaining if remaining > 0 else -2