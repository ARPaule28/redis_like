from typing import Optional, Union, List, Dict
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class StringOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def set(self, key: str, value: str, nx=False, xx=False, 
           ex=None, px=None, keepttl=False) -> Optional[bool]:
        """
        Set key to hold the string value.
        Options:
        - nx: Only set if key doesn't exist
        - xx: Only set if key exists
        - ex: Set expire time in seconds
        - px: Set expire time in milliseconds
        - keepttl: Retain the time to live associated with the key
        """
        if nx and self._data.exists(key):
            return None
        if xx and not self._data.exists(key):
            return None
        
        # Handle TTL preservation
        if not keepttl and key in self._data._expirations:
            del self._data._expirations[key]
            
        self._data._strings[key] = value
        self._data._type_map[key] = 'string'
        
        # Set expiration if specified
        expire_time = None
        if ex is not None:
            expire_time = ex
        elif px is not None:
            expire_time = px / 1000
            
        if expire_time:
            self._data.expire(key, expire_time)
            
        return True
    
    def get(self, key: str) -> Optional[str]:
        """Get the value of key"""
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'string':
            raise WrongTypeError('string', self._data.type(key))
        return self._data._strings.get(key)
    
    def getrange(self, key: str, start: int, end: int) -> str:
        """Get substring of the string stored at key"""
        value = self.get(key)
        if value is None:
            return ""
        length = len(value)
        start = max(start, -length) if start < 0 else start
        end = min(end, length-1) if end >= 0 else length + end
        return value[start:end+1]
    
    def setrange(self, key: str, offset: int, value: str) -> int:
        """Overwrite part of the string at key starting at offset"""
        current = self.get(key) or ""
        if offset > len(current):
            current += '\x00' * (offset - len(current))
        new_value = current[:offset] + value + current[offset+len(value):]
        self.set(key, new_value)
        return len(new_value)
    
    def strlen(self, key: str) -> int:
        """Get length of the value stored at key"""
        value = self.get(key)
        return 0 if value is None else len(value)
    
    def append(self, key: str, value: str) -> int:
        """Append a value to a key"""
        current = self.get(key) or ""
        new_value = current + value
        self.set(key, new_value)
        return len(new_value)
    
    def incr(self, key: str) -> int:
        """Increment the integer value of a key by 1"""
        return self.incrby(key, 1)
    
    def decr(self, key: str) -> int:
        """Decrement the integer value of a key by 1"""
        return self.incrby(key, -1)
    
    def incrby(self, key: str, increment: int) -> int:
        """Increment the integer value of a key by the given amount"""
        try:
            current = int(self.get(key) or 0)
        except (ValueError, TypeError):
            raise WrongTypeError('integer', self._data.type(key))
        new_val = current + increment
        self.set(key, str(new_val))
        return new_val
    
    def incrbyfloat(self, key: str, increment: float) -> float:
        """Increment the float value of a key by the given amount"""
        try:
            current = float(self.get(key) or 0.0)
        except (ValueError, TypeError):
            raise WrongTypeError('float', self._data.type(key))
        new_val = current + increment
        self.set(key, str(new_val))
        return new_val
    
    def getset(self, key: str, value: str) -> Optional[str]:
        """Set key to value and return old value"""
        old_value = self.get(key)
        self.set(key, value)
        return old_value
    
    def mget(self, *keys: str) -> List[Optional[str]]:
        """Get values of multiple keys"""
        return [self.get(key) for key in keys]
    
    def mset(self, items: Dict[str, str]) -> bool:
        """Set multiple keys to multiple values"""
        for key, value in items.items():
            self.set(key, value)
        return True
    
    def msetnx(self, items: Dict[str, str]) -> bool:
        """Set multiple keys to multiple values if none exist"""
        if any(self._data.exists(key) for key in items):
            return False
        self.mset(items)
        return True
    
    def bitcount(self, key: str, start: int = 0, end: int = -1) -> int:
        """Count set bits in a string"""
        value = self.get(key)
        if value is None:
            return 0
        if end == -1:
            end = len(value) - 1
        return bin(int.from_bytes(value[start:end+1].encode(), 'big')).count('1')