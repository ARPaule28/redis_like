import socket
from typing import Any, Optional

class RedisClient:
    def __init__(self, host='localhost', port=6379):
        self._host = host
        self._port = port
        self._socket = None
        self._connect()
    
    def _connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))
    
    def execute(self, command: str, *args) -> Any:
        try:
            cmd_str = f"{command} {' '.join(str(arg) for arg in args)}"
            self._socket.sendall(cmd_str.encode())
            return self._socket.recv(1024).decode()
        except (ConnectionError, OSError):
            self._connect()
            return self.execute(command, *args)
    
    def set(self, key: str, value: str) -> str:
        return self.execute('SET', key, value)
    
    def get(self, key: str) -> Optional[str]:
        response = self.execute('GET', key)
        return None if response == "(nil)" else response
    
    def close(self):
        if self._socket:
            self._socket.close()