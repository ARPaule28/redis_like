from typing import Set, List, Optional
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class SetOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def sadd(self, key: str, *members: str) -> int:
        if key not in self._data._sets:
            self._data._sets[key] = set()
            self._data._type_map[key] = 'set'
        
        added = 0
        for member in members:
            if member not in self._data._sets[key]:
                self._data._sets[key].add(member)
                added += 1
        return added
    
    def srem(self, key: str, *members: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        
        removed = 0
        for member in members:
            if member in self._data._sets[key]:
                self._data._sets[key].remove(member)
                removed += 1
        return removed
    
    def smembers(self, key: str) -> Set[str]:
        if not self._data.exists(key):
            return set()
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        return self._data._sets[key].copy()
    
    def sismember(self, key: str, member: str) -> bool:
        if not self._data.exists(key):
            return False
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        return member in self._data._sets[key]
    
    def scard(self, key: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        return len(self._data._sets[key])
    
    def srandmember(self, key: str, count: int = 1) -> Union[Optional[str], List[str]]:
        if not self._data.exists(key):
            return None if count == 1 else []
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        
        members = list(self._data._sets[key])
        if count == 1:
            return members[0] if members else None
        else:
            return members[:count]
    
    def spop(self, key: str, count: int = 1) -> Union[Optional[str], List[str]]:
        if not self._data.exists(key):
            return None if count == 1 else []
        if self._data.type(key) != 'set':
            raise WrongTypeError('set', self._data.type(key))
        
        if count == 1:
            if not self._data._sets[key]:
                return None
            member = self._data._sets[key].pop()
            return member
        else:
            popped = []
            for _ in range(min(count, len(self._data._sets[key]))):
                popped.append(self._data._sets[key].pop())
            return popped
    
    def sinter(self, *keys: str) -> Set[str]:
        if not keys:
            return set()
        
        sets = []
        for key in keys:
            if not self._data.exists(key):
                return set()
            if self._data.type(key) != 'set':
                raise WrongTypeError('set', self._data.type(key))
            sets.append(self._data._sets[key])
        
        return set.intersection(*sets)
    
    def sunion(self, *keys: str) -> Set[str]:
        if not keys:
            return set()
        
        sets = []
        for key in keys:
            if self._data.exists(key) and self._data.type(key) == 'set':
                sets.append(self._data._sets[key])
        
        return set.union(*sets) if sets else set()
    
    def sdiff(self, *keys: str) -> Set[str]:
        if not keys:
            return set()
        
        sets = []
        for key in keys:
            if self._data.exists(key) and self._data.type(key) == 'set':
                sets.append(self._data._sets[key])
        
        if not sets:
            return set()
        
        return set.difference(*sets)