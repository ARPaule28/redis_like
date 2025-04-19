import socket
import threading
import time
from typing import Dict, List, Optional, Tuple
from core.data_store import DataStore
from core.exceptions import RedisError
from storage.persistence import RDBManager

class ReplicationManager:
    """Handles master-replica replication"""
    def __init__(self, data_store: DataStore):
        self._data = data_store
        self._role = 'master'  # or 'replica'
        self._master_host: Optional[str] = None
        self._master_port: Optional[int] = None
        self._replicas: Dict[Tuple[str, int], socket.socket] = {}
        self._repl_offset = 0
        self._repl_backlog: List[str] = []
        self._repl_backlog_size = 1_000_000  # 1MB
        self._sync_in_progress = False
        self._rdb_manager = RDBManager(data_store)
        self._lock = threading.Lock()

    def configure_as_replica(self, host: str, port: int) -> None:
        """Configure this instance as a replica"""
        with self._lock:
            self._role = 'replica'
            self._master_host = host
            self._master_port = port

    def add_replica(self, replica_socket: socket.socket, addr: Tuple[str, int]) -> None:
        """Add a new replica connection"""
        with self._lock:
            self._replicas[addr] = replica_socket

    def remove_replica(self, addr: Tuple[str, int]) -> None:
        """Remove a replica connection"""
        with self._lock:
            if addr in self._replicas:
                self._replicas[addr].close()
                del self._replicas[addr]

    def propagate_command(self, command: str, *args: str) -> None:
        """Propagate command to all replicas"""
        if self._role != 'master':
            return

        cmd_str = f"{command} {' '.join(args)}\r\n"
        self._repl_backlog.append(cmd_str)
        self._repl_offset += len(cmd_str)
        
        # Trim backlog if needed
        if self._repl_offset > self._repl_backlog_size:
            excess = self._repl_offset - self._repl_backlog_size
            self._repl_backlog = self._repl_backlog[excess//100:]
            self._repl_offset = sum(len(cmd) for cmd in self._repl_backlog)

        with self._lock:
            dead_replicas = []
            for addr, sock in self._replicas.items():
                try:
                    sock.sendall(cmd_str.encode())
                except (ConnectionError, OSError):
                    dead_replicas.append(addr)
            
            for addr in dead_replicas:
                self.remove_replica(addr)

    def sync_with_master(self) -> bool:
        """Synchronize with master (for replicas)"""
        if self._role != 'replica' or not self._master_host or not self._master_port:
            return False

        if self._sync_in_progress:
            return False

        self._sync_in_progress = True
        try:
            # Step 1: Connect to master
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self._master_host, self._master_port))
                
                # Step 2: Send REPLCONF commands
                sock.sendall(b"REPLCONF listening-port 6379\r\n")
                sock.sendall(b"REPLCONF capa eof capa psync2\r\n")
                
                # Step 3: Send PSYNC command
                sock.sendall(b"PSYNC ? -1\r\n")
                
                # Step 4: Handle master response
                response = sock.recv(1024).decode()
                if response.startswith('+FULLRESYNC'):
                    # Handle full resync with RDB transfer
                    parts = response.split()
                    master_replid = parts[1]
                    offset = int(parts[2])
                    
                    # Read RDB file (simplified)
                    rdb_data = b''
                    while True:
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        rdb_data += chunk
                    
                    # Load RDB data
                    temp_file = 'temp_master.rdb'
                    with open(temp_file, 'wb') as f:
                        f.write(rdb_data)
                    self._rdb_manager.load(temp_file)
                    Path(temp_file).unlink()
                    
                    # Update replication state
                    self._repl_offset = offset
                    return True
                
                elif response.startswith('+CONTINUE'):
                    # Partial sync not implemented in this example
                    return False
                else:
                    return False
        finally:
            self._sync_in_progress = False

    def handle_replica_command(self, command: str, args: List[str], 
                             sock: socket.socket) -> str:
        """Handle replication-related commands"""
        if command == 'REPLCONF':
            return self._handle_replconf(args, sock)
        elif command == 'PSYNC':
            return self._handle_psync(args, sock)
        elif command == 'SYNC':
            return self._handle_sync(sock)
        else:
            return "-ERR Unknown replication command\r\n"

    def _handle_replconf(self, args: List[str], sock: socket.socket) -> str:
        """Handle REPLCONF command from replicas"""
        if len(args) >= 2 and args[0] == 'listening-port':
            # Store replica address (simplified)
            replica_addr = sock.getpeername()
            self.add_replica(sock, replica_addr)
            return "+OK\r\n"
        return "+OK\r\n"

    def _handle_psync(self, args: List[str], sock: socket.socket) -> str:
        """Handle PSYNC command from replicas"""
        if len(args) < 2:
            return "-ERR Wrong number of arguments for PSYNC\r\n"
        
        # For simplicity, we always do full resync
        self._rdb_manager.save('temp_sync.rdb')
        with open('temp_sync.rdb', 'rb') as f:
            rdb_data = f.read()
        Path('temp_sync.rdb').unlink()
        
        response = f"+FULLRESYNC 0000000000000000000000000000000000000000 {self._repl_offset}\r\n"
        sock.sendall(response.encode())
        sock.sendall(rdb_data)
        return ""

    def _handle_sync(self, sock: socket.socket) -> str:
        """Handle old SYNC command (for compatibility)"""
        self._rdb_manager.save('temp_sync.rdb')
        with open('temp_sync.rdb', 'rb') as f:
            rdb_data = f.read()
        Path('temp_sync.rdb').unlink()
        
        sock.sendall(b"+SYNC\r\n")
        sock.sendall(rdb_data)
        return ""

    def close_all_replicas(self) -> None:
        """Close all replica connections"""
        with self._lock:
            for sock in self._replicas.values():
                sock.close()
            self._replicas.clear()