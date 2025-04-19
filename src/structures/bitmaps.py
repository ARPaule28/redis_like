from typing import Optional
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError, OutOfRangeError

class BitmapOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def setbit(self, key: str, offset: int, value: int) -> int:
        if value not in (0, 1):
            raise ValueError("Value must be 0 or 1")
        
        if key not in self._data._bitmaps:
            self._data._bitmaps[key] = bytearray()
            self._data._type_map[key] = 'bitmap'
        
        byte_idx = offset // 8
        bit_idx = offset % 8
        
        # Expand bitmap if needed
        while len(self._data._bitmaps[key]) <= byte_idx:
            self._data._bitmaps[key].append(0)
        
        byte_val = self._data._bitmaps[key][byte_idx]
        original = (byte_val >> bit_idx) & 1
        
        if value:
            byte_val |= (1 << bit_idx)
        else:
            byte_val &= ~(1 << bit_idx)
        
        self._data._bitmaps[key][byte_idx] = byte_val
        return original
    
    def getbit(self, key: str, offset: int) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'bitmap':
            raise WrongTypeError('bitmap', self._data.type(key))
        
        byte_idx = offset // 8
        bit_idx = offset % 8
        
        if byte_idx >= len(self._data._bitmaps[key]):
            return 0
        
        byte_val = self._data._bitmaps[key][byte_idx]
        return (byte_val >> bit_idx) & 1
    
    def bitcount(self, key: str, start: int = 0, end: int = -1) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'bitmap':
            raise WrongTypeError('bitmap', self._data.type(key))
        
        bitmap = self._data._bitmaps[key]
        if end == -1:
            end = len(bitmap) - 1
        
        count = 0
        for byte in bitmap[start:end+1]:
            count += bin(byte).count('1')
        return count