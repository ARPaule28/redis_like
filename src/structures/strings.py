from core.data_store import DataStore
from core.exceptions import KeyNotFoundError

class StringOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def set(self, key: str, value: str) -> None:
        self._data.set(key, value, 'string')
    
    def get(self, key: str) -> str:
        try:
            value = self._data.get(key)
            if not isinstance(value, str):
                raise WrongTypeError('string', self._data.type_of(key))
            return value
        except KeyNotFoundError:
            return None
    
    def append(self, key: str, value: str) -> int:
        current = self.get(key) or ""
        new_value = current + value
        self.set(key, new_value)
        return len(new_value)
    
    def incr(self, key: str) -> int:
        try:
            current = int(self.get(key) or 0)
        except ValueError:
            raise RedisError("Value is not an integer")
        new_val = current + 1
        self.set(key, str(new_val))
        return new_val