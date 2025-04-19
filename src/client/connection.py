import socket
from typing import Tuple, Optional
import threading

class Connection:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.host = host
        self.port = port
        self._sock = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish a new connection"""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
    
    def execute(self, command: str, *args) -> str:
        """Execute a command and return the response"""
        try:
            cmd_str = f"{command} {' '.join(str(arg) for arg in args)}\r\n"
            self._sock.sendall(cmd_str.encode())
            return self._read_response()
        except (ConnectionError, OSError):
            self._connect()
            return self.execute(command, *args)
    
    def _read_response(self) -> str:
        """Read server response"""
        response = self._sock.recv(4096).decode().strip()
        if response.startswith('-'):
            raise RuntimeError(response[1:])
        return response
    
    def close(self) -> None:
        """Close the connection"""
        if self._sock:
            self._sock.close()

class ConnectionPool:
    def __init__(self, host: str = 'localhost', port: int = 6379, 
                max_connections: int = 10):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self._pool = []
        self._lock = threading.Lock()
    
    def get_connection(self) -> Connection:
        """Get a connection from the pool"""
        with self._lock:
            if self._pool:
                return self._pool.pop()
            if len(self._pool) < self.max_connections:
                return Connection(self.host, self.port)
            raise RuntimeError("Connection pool exhausted")
    
    def release_connection(self, conn: Connection) -> None:
        """Return a connection to the pool"""
        with self._lock:
            if len(self._pool) < self.max_connections:
                self._pool.append(conn)
            else:
                conn.close()
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        with self._lock:
            for conn in self._pool:
                conn.close()
            self._pool.clear()