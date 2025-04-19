from typing import List, Optional, Union
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError, OutOfRangeError

class ListOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def lpush(self, key: str, *values: str) -> int:
        if key not in self._data._lists:
            self._data._lists[key] = []
            self._data._type_map[key] = 'list'
        self._data._lists[key][0:0] = list(values)
        return len(self._data._lists[key])
    
    def rpush(self, key: str, *values: str) -> int:
        if key not in self._data._lists:
            self._data._lists[key] = []
            self._data._type_map[key] = 'list'
        self._data._lists[key].extend(values)
        return len(self._data._lists[key])
    
    def lpop(self, key: str, count: int = 1) -> Optional[Union[str, List[str]]]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        if count == 1:
            return self._data._lists[key].pop(0) if self._data._lists[key] else None
        else:
            popped = []
            for _ in range(min(count, len(self._data._lists[key]))):
                popped.append(self._data._lists[key].pop(0))
            return popped
    
    def rpop(self, key: str, count: int = 1) -> Optional[Union[str, List[str]]]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        if count == 1:
            return self._data._lists[key].pop() if self._data._lists[key] else None
        else:
            popped = []
            for _ in range(min(count, len(self._data._lists[key]))):
                popped.append(self._data._lists[key].pop())
            return popped
    
    def llen(self, key: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        return len(self._data._lists[key])
    
    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        lst = self._data._lists[key]
        start = max(start, -len(lst)) if start < 0 else start
        stop = min(stop, len(lst)-1) if stop >= 0 else len(lst) + stop
        return lst[start:stop+1]
    
    def lindex(self, key: str, index: int) -> Optional[str]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        try:
            return self._data._lists[key][index]
        except IndexError:
            return None
    
    def lset(self, key: str, index: int, value: str) -> bool:
        if not self._data.exists(key):
            raise KeyNotFoundError(key)
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        try:
            self._data._lists[key][index] = value
            return True
        except IndexError:
            raise OutOfRangeError("Index out of range")
    
    def ltrim(self, key: str, start: int, stop: int) -> bool:
        if not self._data.exists(key):
            return False
        if self._data.type(key) != 'list':
            raise WrongTypeError('list', self._data.type(key))
        
        lst = self._data._lists[key]
        start = max(start, -len(lst)) if start < 0 else start
        stop = min(stop, len(lst)-1) if stop >= 0 else len(lst) + stop
        
        self._data._lists[key] = lst[start:stop+1]
        return True