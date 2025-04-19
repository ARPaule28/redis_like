from typing import Any, List, Dict, Optional, Union
from .connection import ConnectionPool

class RedisClient:
    def __init__(self, host: str = 'localhost', port: int = 6379, 
                pool_size: int = 5):
        self.pool = ConnectionPool(host, port, pool_size)
    
    def _execute(self, command: str, *args) -> Any:
        """Execute a command using connection pool"""
        conn = self.pool.get_connection()
        try:
            response = conn.execute(command, *args)
            return self._parse_response(response)
        finally:
            self.pool.release_connection(conn)
    
    def _parse_response(self, response: str) -> Any:
        """Parse server response"""
        if response == 'nil':
            return None
        try:
            return int(response)
        except ValueError:
            try:
                return float(response)
            except ValueError:
                if response.startswith('[') and response.endswith(']'):
                    return [self._parse_response(item) 
                           for item in response[1:-1].split(',')]
                return response
    
    # Key commands
    def set(self, key: str, value: str, **kwargs) -> bool:
        """Set key to hold the string value"""
        args = [key, value]
        if kwargs.get('nx'):
            args.append('NX')
        if kwargs.get('xx'):
            args.append('XX')
        if kwargs.get('ex'):
            args.extend(['EX', str(kwargs['ex'])])
        return bool(self._execute('SET', *args))
    
    def get(self, key: str) -> Optional[str]:
        """Get the value of key"""
        return self._execute('GET', key)
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys"""
        return self._execute('DEL', *keys)
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        return bool(self._execute('EXISTS', key))
    
    # String commands
    def incr(self, key: str) -> int:
        """Increment the integer value of key by 1"""
        return self._execute('INCR', key)
    
    def decr(self, key: str) -> int:
        """Decrement the integer value of key by 1"""
        return self._execute('DECR', key)
    
    def append(self, key: str, value: str) -> int:
        """Append a value to a key"""
        return self._execute('APPEND', key, value)
    
    # List commands
    def lpush(self, key: str, *values: str) -> int:
        """Prepend one or multiple values to a list"""
        return self._execute('LPUSH', key, *values)
    
    def rpush(self, key: str, *values: str) -> int:
        """Append one or multiple values to a list"""
        return self._execute('RPUSH', key, *values)
    
    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Get a range of elements from a list"""
        return self._execute('LRANGE', key, start, stop)
    
    # Hash commands
    def hset(self, key: str, field: str, value: str) -> int:
        """Set field in hash stored at key to value"""
        return self._execute('HSET', key, field, value)
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get value of a field in hash"""
        return self._execute('HGET', key, field)
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all fields and values in a hash"""
        response = self._execute('HGETALL', key)
        return dict(zip(response[::2], response[1::2])) if response else {}
    
    # Set commands
    def sadd(self, key: str, *members: str) -> int:
        """Add one or more members to a set"""
        return self._execute('SADD', key, *members)
    
    def smembers(self, key: str) -> Set[str]:
        """Get all members in a set"""
        return set(self._execute('SMEMBERS', key))
    
    def close(self) -> None:
        """Close all connections in the pool"""
        self.pool.close_all()