import bisect
from typing import Dict, List, Optional, Set, Tuple
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class SortedSetOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def zadd(self, key: str, score: float, member: str) -> int:
        if key not in self._data._sorted_sets:
            self._data._sorted_sets[key] = {}
            self._data._type_map[key] = 'zset'
        
        is_new = member not in self._data._sorted_sets[key]
        self._data._sorted_sets[key][member] = score
        return 1 if is_new else 0
    
    def zrange(self, key: str, start: int, stop: int, 
              with_scores: bool = False) -> List[Union[str, Tuple[str, float]]]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        
        members_scores = sorted(
            self._data._sorted_sets[key].items(),
            key=lambda x: (x[1], x[0])
        )
        
        # Handle negative indices
        if start < 0:
            start = max(0, len(members_scores) + start)
        if stop < 0:
            stop = len(members_scores) + stop
        
        result = members_scores[start:stop+1]
        
        if with_scores:
            return [(member, score) for member, score in result]
        return [member for member, _ in result]
    
    def zrevrange(self, key: str, start: int, stop: int,
                 with_scores: bool = False) -> List[Union[str, Tuple[str, float]]]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        
        members_scores = sorted(
            self._data._sorted_sets[key].items(),
            key=lambda x: (-x[1], x[0])
        )
        
        # Handle negative indices
        if start < 0:
            start = max(0, len(members_scores) + start)
        if stop < 0:
            stop = len(members_scores) + stop
        
        result = members_scores[start:stop+1]
        
        if with_scores:
            return [(member, score) for member, score in result]
        return [member for member, _ in result]
    
    def zrank(self, key: str, member: str) -> Optional[int]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        if member not in self._data._sorted_sets[key]:
            return None
        
        members_scores = sorted(
            self._data._sorted_sets[key].items(),
            key=lambda x: (x[1], x[0])
        )
        
        for i, (m, _) in enumerate(members_scores):
            if m == member:
                return i
        return None
    
    def zrevrank(self, key: str, member: str) -> Optional[int]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        if member not in self._data._sorted_sets[key]:
            return None
        
        members_scores = sorted(
            self._data._sorted_sets[key].items(),
            key=lambda x: (-x[1], x[0])
        )
        
        for i, (m, _) in enumerate(members_scores):
            if m == member:
                return i
        return None
    
    def zscore(self, key: str, member: str) -> Optional[float]:
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        return self._data._sorted_sets[key].get(member)
    
    def zcard(self, key: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        return len(self._data._sorted_sets[key])
    
    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        
        count = 0
        for score in self._data._sorted_sets[key].values():
            if min_score <= score <= max_score:
                count += 1
        return count
    
    def zrem(self, key: str, *members: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'zset':
            raise WrongTypeError('zset', self._data.type(key))
        
        removed = 0
        for member in members:
            if member in self._data._sorted_sets[key]:
                del self._data._sorted_sets[key][member]
                removed += 1
        return removed
    
    def zincrby(self, key: str, increment: float, member: str) -> float:
        if key not in self._data._sorted_sets:
            self._data._sorted_sets[key] = {}
            self._data._type_map[key] = 'zset'
        
        current = self._data._sorted_sets[key].get(member, 0)
        new_score = current + increment
        self._data._sorted_sets[key][member] = new_score
        return new_score