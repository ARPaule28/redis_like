import time
from typing import Dict, List, Optional, Tuple
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class StreamEntry:
    def __init__(self, stream_id: str, fields: Dict[str, str]):
        self.id = stream_id
        self.fields = fields

class StreamOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
    
    def xadd(self, key: str, fields: Dict[str, str], 
            stream_id: str = '*') -> str:
        if key not in self._data._streams:
            self._data._streams[key] = []
            self._data._type_map[key] = 'stream'
        
        if stream_id == '*':
            timestamp = int(time.time() * 1000)
            last_entry = self._data._streams[key][-1] if self._data._streams[key] else None
            sequence = 0
            if last_entry and last_entry[0].startswith(str(timestamp)):
                sequence = int(last_entry[0].split('-')[1]) + 1
            stream_id = f"{timestamp}-{sequence}"
        
        self._data._streams[key].append((stream_id, fields))
        return stream_id
    
    def xrange(self, key: str, start: str, end: str, 
              count: Optional[int] = None) -> List[Tuple[str, Dict[str, str]]]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'stream':
            raise WrongTypeError('stream', self._data.type(key))
        
        results = []
        for entry_id, fields in self._data._streams[key]:
            if start <= entry_id <= end:
                results.append((entry_id, fields))
                if count and len(results) >= count:
                    break
        return results
    
    def xrevrange(self, key: str, end: str, start: str,
                 count: Optional[int] = None) -> List[Tuple[str, Dict[str, str]]]:
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'stream':
            raise WrongTypeError('stream', self._data.type(key))
        
        results = []
        for entry_id, fields in reversed(self._data._streams[key]):
            if start <= entry_id <= end:
                results.append((entry_id, fields))
                if count and len(results) >= count:
                    break
        return results
    
    def xlen(self, key: str) -> int:
        if not self._data.exists(key):
            return 0
        if self._data.type(key) != 'stream':
            raise WrongTypeError('stream', self._data.type(key))
        return len(self._data._streams[key])
    
    def xread(self, streams: Dict[str, str], count: Optional[int] = None,
             block: Optional[int] = None) -> Dict[str, List[Tuple[str, Dict[str, str]]]]:
        results = {}
        for stream_key, last_id in streams.items():
            if not self._data.exists(stream_key):
                continue
            if self._data.type(stream_key) != 'stream':
                raise WrongTypeError('stream', self._data.type(stream_key))
            
            stream_entries = []
            for entry_id, fields in self._data._streams[stream_key]:
                if entry_id > last_id:
                    stream_entries.append((entry_id, fields))
                    if count and len(stream_entries) >= count:
                        break
            
            if stream_entries:
                results[stream_key] = stream_entries
        
        return results