import time
from typing import Dict, List, Optional

class StreamEntry:
    def __init__(self, stream_id: str, fields: Dict[str, str]):
        self.id = stream_id
        self.fields = fields

class StreamOps:
    def __init__(self):
        self._streams: Dict[str, List[StreamEntry]] = {}
        self._groups: Dict[str, Dict[str, dict]] = {}  # {stream: {group: data}}
    
    def xadd(self, stream_key: str, fields: Dict[str, str], 
             stream_id: str = '*') -> str:
        if stream_key not in self._streams:
            self._streams[stream_key] = []
        
        if stream_id == '*':
            timestamp = int(time.time() * 1000)
            last_entry = self._streams[stream_key][-1] if self._streams[stream_key] else None
            sequence = 0
            if last_entry and last_entry.id.startswith(str(timestamp)):
                sequence = int(last_entry.id.split('-')[1]) + 1
            stream_id = f"{timestamp}-{sequence}"
        
        entry = StreamEntry(stream_id, fields)
        self._streams[stream_key].append(entry)
        return stream_id
    
    def xrange(self, stream_key: str, start: str, end: str) -> List[StreamEntry]:
        if stream_key not in self._streams:
            return []
        
        entries = []
        for entry in self._streams[stream_key]:
            if start <= entry.id <= end:
                entries.append(entry)
        return entries