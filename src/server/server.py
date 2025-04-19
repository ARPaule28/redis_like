import socket
import threading
from typing import Dict, Any
from core.data_store import DataStore
from .handlers import CommandHandler

class RedisServer:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.host = host
        self.port = port
        self._data_store = DataStore()
        self._handler = CommandHandler(self._data_store)
        self._running = False
        self._threads = []
        self._cleaner_thread = threading.Thread(
            target=self._clean_expired_keys, 
            daemon=True
        )
    
    def start(self) -> None:
        """Start the Redis server"""
        self._running = True
        self._cleaner_thread.start()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            print(f"Server started on {self.host}:{self.port}")
            
            while self._running:
                try:
                    conn, addr = s.accept()
                    thread = threading.Thread(
                        target=self._handle_connection,
                        args=(conn, addr)
                    )
                    self._threads.append(thread)
                    thread.start()
                except OSError:
                    break  # Server is shutting down
    
    def stop(self) -> None:
        """Stop the Redis server"""
        self._running = False
        # Create a dummy connection to unblock accept()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
        except:
            pass
        
        for thread in self._threads:
            thread.join()
    
    def _handle_connection(self, conn: socket.socket, addr: Any) -> None:
        """Handle a client connection"""
        with conn:
            print(f"New connection from {addr}")
            while self._running:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    # Parse Redis protocol (simplified)
                    parts = data.decode().strip().split()
                    if not parts:
                        continue
                    
                    command = parts[0]
                    args = parts[1:]
                    
                    # Handle command
                    response = self._handler.handle_command(command, args)
                    conn.sendall(response.encode())
                except (ConnectionError, UnicodeDecodeError):
                    break
                except Exception as e:
                    error_msg = f"-ERR {str(e)}\r\n"
                    conn.sendall(error_msg.encode())
    
    def _clean_expired_keys(self) -> None:
        """Background task to clean expired keys"""
        import time
        while self._running:
            time.sleep(1)
            self._data_store._clean_expired()