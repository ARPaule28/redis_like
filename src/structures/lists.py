from collections import deque
from core.data_store import DataStore

class ListOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
        self._lists: Dict[str, deque] = {}
    
    def lpush(self, key: str, *values: str) -> int:
        if key not in self._lists:
            self._lists[key] = deque()
            self._data.set(key, None, 'list')
        self._lists[key].extendleft(reversed(values))
        return len(self._lists[key])
    
    def rpush(self, key: str, *values: str) -> int:
        if key not in self._lists:
            self._lists[key] = deque()
            self._data.set(key, None, 'list')
        self._lists[key].extend(values)
        return len(self._lists[key])
    
    def lrange(self, key: str, start: int, end: int) -> list:
        lst = list(self._lists.get(key, deque()))
        return lst[start:end+1] if end != -1 else lst[start:]