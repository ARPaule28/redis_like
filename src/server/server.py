import socket
import threading
from typing import Dict, Callable
from core.data_store import DataStore
from structures import (
    StringOps, ListOps, HashOps, 
    SetOps, SortedSetOps, StreamOps
)

class RedisServer:
    def __init__(self):
        self._data_store = DataStore()
        self._string_ops = StringOps(self._data_store)
        self._list_ops = ListOps(self._data_store)
        self._stream_ops = StreamOps()
        self._commands: Dict[str, Callable] = self._register_commands()
    
    def _register_commands(self) -> Dict[str, Callable]:
        return {
            # String commands
            'SET': self._string_ops.set,
            'GET': self._string_ops.get,
            'APPEND': self._string_ops.append,
            'INCR': self._string_ops.incr,
            
            # List commands
            'LPUSH': self._list_ops.lpush,
            'RPUSH': self._list_ops.rpush,
            'LRANGE': self._list_ops.lrange,
            
            # Stream commands
            'XADD': self._stream_ops.xadd,
            'XRANGE': self._stream_ops.xrange,
        }
    
    def handle_command(self, command: str, *args) -> str:
        cmd = command.upper()
        if cmd not in self._commands:
            return f"ERR unknown command '{command}'"
        
        try:
            result = self._commands[cmd](*args)
            return str(result) if result is not None else "(nil)"
        except Exception as e:
            return f"ERR {str(e)}"
    
    def start(self, host='localhost', port=6379):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen()
            print(f"Server running on {host}:{port}")
            
            while True:
                conn, addr = s.accept()
                threading.Thread(
                    target=self._handle_connection, 
                    args=(conn,)
                ).start()
    
    def _handle_connection(self, conn):
        with conn:
            while True:
                try:
                    data = conn.recv(1024).decode().strip()
                    if not data:
                        break
                    
                    parts = data.split()
                    command = parts[0]
                    args = parts[1:]
                    
                    response = self.handle_command(command, *args)
                    conn.sendall(response.encode())
                except ConnectionError:
                    break