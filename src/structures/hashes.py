from typing import Dict, List, Optional
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class HashOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def hset(self, key: str, field: str, value: str) -> int:
        if key not in self._data._hashes:
            self._data._hashes[key] = {}
            self._data._type_map[key] = 'hash'
        
        is_new = field not in self._data._hashes[key]
        self._data._hashes[key][field] = value
        return 1 if is_new else 0
    
    def hget(self, key: str, field: str) -> Optional[str]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return self._data._hashes[key].get(field)
    
    def hgetall(self, key: str) -> Dict[str, str]:
        if not self._data.exists(key):
            return {}
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return self._data._hashes[key].copy()
    
    def hdel(self, key: str, *fields: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        
        count = 0
        for field in fields:
            if field in self._data._hashes[key]:
                del self._data._hashes[key][field]
                count += 1
        return count
    
    def hexists(self, key: str, field: str) -> bool:
        if not self._data.exists(key):
            return False
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return field in self._data._hashes[key]
    
    def hkeys(self, key: str) -> List[str]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return list(self._data._hashes[key].keys())
    
    def hvals(self, key: str) -> List[str]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return list(self._data._hashes[key].values())
    
    def hlen(self, key: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'hash':
            raise WrongTypeError('hash', self._data.type(key))
        return len(self._data._hashes[key])