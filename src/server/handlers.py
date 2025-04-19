from typing import Dict, Any, List, Optional
from core.data_store import DataStore
from structures import (
    StringOps, ListOps, SetOps, 
    HashOps, SortedSetOps, StreamOps,
    BitmapOps, GeoOps, VectorOps,
    TimeSeriesOps
)

class CommandHandler:
    def __init__(self, data_store: DataStore):
        self._data = data_store
        self._handlers = {
            # Key commands
            'DEL': self._delete,
            'EXISTS': self._exists,
            'EXPIRE': self._expire,
            'TTL': self._ttl,
            'TYPE': self._type,
            
            # String commands
            'SET': self._set,
            'GET': self._get,
            'INCR': self._incr,
            'DECR': self._decr,
            'APPEND': self._append,
            'STRLEN': self._strlen,
            
            # List commands
            'LPUSH': self._lpush,
            'RPUSH': self._rpush,
            'LPOP': self._lpop,
            'RPOP': self._rpop,
            'LRANGE': self._lrange,
            'LLEN': self._llen,
            
            # Hash commands
            'HSET': self._hset,
            'HGET': self._hget,
            'HGETALL': self._hgetall,
            'HDEL': self._hdel,
            'HEXISTS': self._hexists,
            
            # Set commands
            'SADD': self._sadd,
            'SREM': self._srem,
            'SMEMBERS': self._smembers,
            'SISMEMBER': self._sismember,
            'SCARD': self._scard,
            
            # Sorted set commands
            'ZADD': self._zadd,
            'ZRANGE': self._zrange,
            'ZREM': self._zrem,
            'ZCARD': self._zcard,
            
            # Stream commands
            'XADD': self._xadd,
            'XRANGE': self._xrange,
            'XLEN': self._xlen,
            
            # Bitmap commands
            'SETBIT': self._setbit,
            'GETBIT': self._getbit,
            'BITCOUNT': self._bitcount,
            
            # Geo commands
            'GEOADD': self._geoadd,
            'GEODIST': self._geodist,
            'GEORADIUS': self._georadius,
            
            # Vector commands
            'VECADD': self._vecadd,
            'VECGET': self._vecget,
            'VECSEARCH': self._vecsearch,
            
            # Time series commands
            'TSADD': self._tsadd,
            'TSGET': self._tsget,
            'TSRANGE': self._tsrange,
        }
        
        # Initialize operation handlers
        self._string_ops = StringOps(data_store)
        self._list_ops = ListOps(data_store)
        self._set_ops = SetOps(data_store)
        self._hash_ops = HashOps(data_store)
        self._zset_ops = SortedSetOps(data_store)
        self._stream_ops = StreamOps(data_store)
        self._bitmap_ops = BitmapOps(data_store)
        self._geo_ops = GeoOps(data_store)
        self._vector_ops = VectorOps(data_store)
        self._ts_ops = TimeSeriesOps(data_store)
    
    def handle_command(self, command: str, args: List[str]) -> str:
        """Handle a command with arguments"""
        cmd = command.upper()
        if cmd not in self._handlers:
            return f"-ERR unknown command '{command}'\r\n"
        
        try:
            result = self._handlers[cmd](*args)
            return self._format_response(result)
        except Exception as e:
            return f"-ERR {str(e)}\r\n"
    
    def _format_response(self, result: Any) -> str:
        """Format response according to Redis protocol"""
        if result is None:
            return "$-1\r\n"  # nil
        elif isinstance(result, bool):
            return ":1\r\n" if result else ":0\r\n"
        elif isinstance(result, int):
            return f":{result}\r\n"
        elif isinstance(result, str):
            return f"${len(result)}\r\n{result}\r\n"
        elif isinstance(result, (list, tuple)):
            response = f"*{len(result)}\r\n"
            for item in result:
                response += self._format_response(item)
            return response
        elif isinstance(result, dict):
            response = f"*{len(result)*2}\r\n"
            for k, v in result.items():
                response += self._format_response(k)
                response += self._format_response(v)
            return response
        else:
            return f"${len(str(result))}\r\n{result}\r\n"
    
    # Key command implementations
    def _delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if self._data.delete(key):
                count += 1
        return count
    
    def _exists(self, key: str) -> int:
        return int(self._data.exists(key))
    
    def _expire(self, key: str, seconds: str) -> int:
        try:
            return int(self._data.expire(key, float(seconds)))
        except ValueError:
            raise ValueError("Invalid expiration time")
    
    def _ttl(self, key: str) -> int:
        ttl = self._data.ttl(key)
        return int(ttl) if ttl is not None else -2
    
    def _type(self, key: str) -> str:
        return self._data.type(key)
    
    # String command implementations
    def _set(self, key: str, value: str, *options) -> bool:
        nx = 'NX' in options
        xx = 'XX' in options
        ex = None
        if 'EX' in options:
            idx = options.index('EX')
            ex = float(options[idx+1])
        return self._string_ops.set(key, value, nx=nx, xx=xx, ex=ex)
    
    def _get(self, key: str) -> Optional[str]:
        return self._string_ops.get(key)
    
    def _incr(self, key: str) -> int:
        return self._string_ops.incr(key)
    
    def _decr(self, key: str) -> int:
        return self._string_ops.decr(key)
    
    def _append(self, key: str, value: str) -> int:
        return self._string_ops.append(key, value)
    
    def _strlen(self, key: str) -> int:
        return self._string_ops.strlen(key)
    
    # List command implementations
    def _lpush(self, key: str, *values: str) -> int:
        return self._list_ops.lpush(key, *values)
    
    def _rpush(self, key: str, *values: str) -> int:
        return self._list_ops.rpush(key, *values)
    
    def _lpop(self, key: str, count: str = '1') -> Union[str, List[str]]:
        return self._list_ops.lpop(key, int(count))
    
    def _rpop(self, key: str, count: str = '1') -> Union[str, List[str]]:
        return self._list_ops.rpop(key, int(count))
    
    def _lrange(self, key: str, start: str, stop: str) -> List[str]:
        return self._list_ops.lrange(key, int(start), int(stop))
    
    def _llen(self, key: str) -> int:
        return self._list_ops.llen(key)
    
    # Hash command implementations
    def _hset(self, key: str, field: str, value: str) -> int:
        return self._hash_ops.hset(key, field, value)
    
    def _hget(self, key: str, field: str) -> Optional[str]:
        return self._hash_ops.hget(key, field)
    
    def _hgetall(self, key: str) -> Dict[str, str]:
        return self._hash_ops.hgetall(key)
    
    def _hdel(self, key: str, *fields: str) -> int:
        return self._hash_ops.hdel(key, *fields)
    
    def _hexists(self, key: str, field: str) -> int:
        return int(self._hash_ops.hexists(key, field))
    
    # Set command implementations
    def _sadd(self, key: str, *members: str) -> int:
        return self._set_ops.sadd(key, *members)
    
    def _srem(self, key: str, *members: str) -> int:
        return self._set_ops.srem(key, *members)
    
    def _smembers(self, key: str) -> List[str]:
        return list(self._set_ops.smembers(key))
    
    def _sismember(self, key: str, member: str) -> int:
        return int(self._set_ops.sismember(key, member))
    
    def _scard(self, key: str) -> int:
        return self._set_ops.scard(key)
    
    # Sorted set command implementations
    def _zadd(self, key: str, score: str, member: str) -> int:
        return self._zset_ops.zadd(key, float(score), member)
    
    def _zrange(self, key: str, start: str, stop: str, *options) -> List[str]:
        with_scores = 'WITHSCORES' in options
        return self._zset_ops.zrange(key, int(start), int(stop), with_scores)
    
    def _zrem(self, key: str, *members: str) -> int:
        return self._zset_ops.zrem(key, *members)
    
    def _zcard(self, key: str) -> int:
        return self._zset_ops.zcard(key)
    
    # Stream command implementations
    def _xadd(self, key: str, *args) -> str:
        if len(args) % 2 != 0:
            raise ValueError("Wrong number of arguments for XADD")
        fields = dict(zip(args[::2], args[1::2]))
        return self._stream_ops.xadd(key, fields)
    
    def _xrange(self, key: str, start: str, end: str) -> List[Any]:
        return self._stream_ops.xrange(key, start, end)
    
    def _xlen(self, key: str) -> int:
        return self._stream_ops.xlen(key)
    
    # Bitmap command implementations
    def _setbit(self, key: str, offset: str, value: str) -> int:
        return self._bitmap_ops.setbit(key, int(offset), int(value))
    
    def _getbit(self, key: str, offset: str) -> int:
        return self._bitmap_ops.getbit(key, int(offset))
    
    def _bitcount(self, key: str, *args) -> int:
        if not args:
            return self._bitmap_ops.bitcount(key)
        return self._bitmap_ops.bitcount(key, int(args[0]), int(args[1]))
    
    # Geo command implementations
    def _geoadd(self, key: str, longitude: str, latitude: str, member: str) -> int:
        return self._geo_ops.geoadd(key, float(longitude), float(latitude), member)
    
    def _geodist(self, key: str, member1: str, member2: str, unit: str = 'km') -> Optional[float]:
        return self._geo_ops.geodist(key, member1, member2, unit)
    
    def _georadius(self, key: str, longitude: str, latitude: str, 
                  radius: str, unit: str = 'km') -> List[str]:
        return self._geo_ops.georadius(
            key, float(longitude), float(latitude), float(radius), unit)
    
    # Vector command implementations
    def _vecadd(self, key: str, *vector: str) -> bool:
        vec = list(map(float, vector))
        return self._vector_ops.vec_add(key, vec)
    
    def _vecget(self, key: str) -> Optional[List[float]]:
        return self._vector_ops.vec_get(key)
    
    def _vecsearch(self, *query: str, k: str = '5') -> List[Tuple[str, float]]:
        vec = list(map(float, query))
        return self._vector_ops.vec_search(vec, int(k))
    
    # Time series command implementations
    def _tsadd(self, key: str, value: str, timestamp: str = None) -> float:
        ts = float(timestamp) if timestamp else None
        return self._ts_ops.ts_add(key, float(value), ts)
    
    def _tsget(self, key: str) -> Optional[Tuple[float, float]]:
        return self._ts_ops.ts_get(key)
    
    def _tsrange(self, key: str, start: str, end: str) -> List[Tuple[float, float]]:
        return self._ts_ops.ts_range(key, float(start), float(end))