import time
import threading
from collections import defaultdict
from typing import Dict, Any, Optional, Union, List, Set, Tuple
from .exceptions import KeyNotFoundError, WrongTypeError

class DataStore:
    def __init__(self):
        # Main storage
        self._strings: Dict[str, str] = {}
        self._lists: Dict[str, List[str]] = {}
        self._sets: Dict[str, Set[str]] = {}
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._sorted_sets: Dict[str, Dict[str, float]] = {}
        self._streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self._bitmaps: Dict[str, bytearray] = {}
        self._geo: Dict[str, Dict[str, Tuple[float, float]]] = {}
        self._vectors: Dict[str, List[float]] = {}
        
        # Metadata
        self._expirations: Dict[str, float] = {}
        self._type_map: Dict[str, str] = {}
        self._locks = defaultdict(threading.Lock)
    
    # Common operations
    def exists(self, key: str) -> bool:
        with self._locks[key]:
            return key in self._type_map and not self._is_expired(key)
    
    def type(self, key: str) -> str:
        with self._locks[key]:
            if not self.exists(key):
                return 'none'
            return self._type_map[key]
    
    def delete(self, key: str) -> bool:
        with self._locks[key]:
            if key not in self._type_map:
                return False
            
            data_type = self._type_map[key]
            if data_type == 'string':
                del self._strings[key]
            elif data_type == 'list':
                del self._lists[key]
            elif data_type == 'set':
                del self._sets[key]
            elif data_type == 'hash':
                del self._hashes[key]
            elif data_type == 'zset':
                del self._sorted_sets[key]
            elif data_type == 'stream':
                del self._streams[key]
            elif data_type == 'bitmap':
                del self._bitmaps[key]
            elif data_type == 'geo':
                del self._geo[key]
            elif data_type == 'vector':
                del self._vectors[key]
            
            if key in self._expirations:
                del self._expirations[key]
            del self._type_map[key]
            return True
    
    def expire(self, key: str, seconds: float) -> bool:
        with self._locks[key]:
            if not self.exists(key):
                return False
            self._expirations[key] = time.time() + seconds
            return True
    
    def ttl(self, key: str) -> Optional[float]:
        with self._locks[key]:
            if key not in self._expirations:
                return None
            if not self.exists(key):
                return -2
            remaining = self._expirations[key] - time.time()
            return remaining if remaining > 0 else -2
    
    def _is_expired(self, key: str) -> bool:
        return key in self._expirations and self._expirations[key] <= time.time()
    
    def _clean_expired(self):
        now = time.time()
        expired = [k for k, t in self._expirations.items() if t <= now]
        for key in expired:
            self.delete(key)