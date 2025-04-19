import json
import pickle
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional
from core.data_store import DataStore
from core.exceptions import RedisError

class PersistenceManager:
    """Base persistence manager"""
    def __init__(self, data_store: DataStore):
        self._data = data_store
        self._lock = threading.Lock()

    def save(self, filename: str) -> bool:
        """Save the database state"""
        raise NotImplementedError

    def load(self, filename: str) -> bool:
        """Load the database state"""
        raise NotImplementedError

class RDBManager(PersistenceManager):
    """Redis Database File (snapshot) persistence"""
    def __init__(self, data_store: DataStore):
        super().__init__(data_store)
        self._last_save = 0

    def save(self, filename: str) -> bool:
        """Save snapshot to RDB file"""
        with self._lock:
            try:
                data = {
                    'strings': self._data._strings,
                    'lists': self._data._lists,
                    'sets': self._data._sets,
                    'hashes': self._data._hashes,
                    'sorted_sets': self._data._sorted_sets,
                    'streams': self._data._streams,
                    'bitmaps': self._data._bitmaps,
                    'geo': self._data._geo,
                    'vectors': self._data._vectors,
                    'expirations': self._data._expirations,
                    'type_map': self._data._type_map
                }
                with open(filename, 'wb') as f:
                    pickle.dump(data, f)
                self._last_save = time.time()
                return True
            except Exception as e:
                raise RedisError(f"RDB save failed: {str(e)}")

    def load(self, filename: str) -> bool:
        """Load from RDB file"""
        with self._lock:
            try:
                if not Path(filename).exists():
                    return False

                with open(filename, 'rb') as f:
                    data = pickle.load(f)

                self._data._strings = data.get('strings', {})
                self._data._lists = data.get('lists', {})
                self._data._sets = data.get('sets', {})
                self._data._hashes = data.get('hashes', {})
                self._data._sorted_sets = data.get('sorted_sets', {})
                self._data._streams = data.get('streams', {})
                self._data._bitmaps = data.get('bitmaps', {})
                self._data._geo = data.get('geo', {})
                self._data._vectors = data.get('vectors', {})
                self._data._expirations = data.get('expirations', {})
                self._data._type_map = data.get('type_map', {})
                return True
            except Exception as e:
                raise RedisError(f"RDB load failed: {str(e)}")

    def needs_save(self, changes_since_last: int) -> bool:
        """Check if save is needed based on changes"""
        return changes_since_last > 0 and time.time() - self._last_save > 60

class AOFManager(PersistenceManager):
    """Append-Only File persistence"""
    def __init__(self, data_store: DataStore, filename: str = 'appendonly.aof'):
        super().__init__(data_store)
        self.filename = filename
        self._file = None
        self._rewrite_in_progress = False
        self._open_file()

    def _open_file(self) -> None:
        """Open or reopen the AOF file"""
        if self._file and not self._file.closed:
            self._file.close()
        self._file = open(self.filename, 'a+')

    def log_command(self, command: str, *args: str) -> None:
        """Append command to AOF"""
        with self._lock:
            try:
                self._file.write(f"{command} {' '.join(args)}\n")
                self._file.flush()
            except IOError:
                self._open_file()
                self._file.write(f"{command} {' '.join(args)}\n")
                self._file.flush()

    def replay(self) -> None:
        """Replay AOF to rebuild state"""
        with self._lock:
            self._file.seek(0)
            for line in self._file:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                command = parts[0]
                args = parts[1:]
                try:
                    self._data._handler.handle_command(command, args)
                except Exception as e:
                    print(f"Error replaying command '{line}': {str(e)}")

    def rewrite(self) -> bool:
        """Rewrite AOF to compact it"""
        if self._rewrite_in_progress:
            return False
        
        self._rewrite_in_progress = True
        try:
            # Create temporary RDB snapshot
            temp_rdb = 'temp_rewrite.rdb'
            rdb = RDBManager(self._data)
            rdb.save(temp_rdb)
            
            # Replace AOF with commands to rebuild current state
            self._file.close()
            Path(self.filename).unlink()
            
            self._file = open(self.filename, 'w')
            rdb.load(temp_rdb)
            
            # Generate minimal commands to recreate state
            self._generate_minimal_aof()
            
            Path(temp_rdb).unlink()
            return True
        finally:
            self._rewrite_in_progress = False
            self._open_file()

    def _generate_minimal_aof(self) -> None:
        """Generate minimal AOF commands for current state"""
        # Strings
        for key, value in self._data._strings.items():
            self.log_command('SET', key, value)
        
        # Lists
        for key, lst in self._data._lists.items():
            if lst:
                self.log_command('RPUSH', key, *lst)
        
        # Sets
        for key, members in self._data._sets.items():
            if members:
                self.log_command('SADD', key, *members)
        
        # Hashes
        for key, fields in self._data._hashes.items():
            for field, value in fields.items():
                self.log_command('HSET', key, field, value)
        
        # Sorted Sets
        for key, members in self._data._sorted_sets.items():
            for member, score in members.items():
                self.log_command('ZADD', key, str(score), member)
        
        # Streams (simplified)
        for key, entries in self._data._streams.items():
            for entry_id, fields in entries:
                field_args = []
                for k, v in fields.items():
                    field_args.extend([k, v])
                self.log_command('XADD', key, entry_id, *field_args)
        
        # Expirations
        for key, ts in self._data._expirations.items():
            ttl = ts - time.time()
            if ttl > 0:
                self.log_command('EXPIRE', key, str(ttl))

    def save(self, filename: Optional[str] = None) -> bool:
        """AOF doesn't need explicit save, but we can rewrite"""
        return self.rewrite()

    def load(self, filename: Optional[str] = None) -> bool:
        """Load by replaying AOF"""
        if filename:
            self.filename = filename
        self.replay()
        return True

    def close(self) -> None:
        """Close the AOF file"""
        if self._file and not self._file.closed:
            self._file.close()