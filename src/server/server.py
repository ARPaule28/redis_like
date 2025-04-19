import socket
import threading
import time
import ssl
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
from core.data_store import DataStore
from .handlers import CommandHandler
from storage.persistence import AOFManager, RDBManager
from storage.replication import ReplicationManager
from utils.monitoring import Monitoring
from utils.security import Authenticator, ACLManager, TLSWrapper

class RedisServer:
    def __init__(self, config: Dict[str, Any] = None):
        # Default configuration
        self.config = {
            'host': 'localhost',
            'port': 6379,
            'aof_enabled': True,
            'aof_file': 'appendonly.aof',
            'rdb_enabled': True,
            'rdb_file': 'dump.rdb',
            'requirepass': None,
            'tls_enabled': False,
            'tls_cert_file': None,
            'tls_key_file': None,
            'tls_ca_cert_file': None,
            'tls_require_client_cert': False,
            'replicaof': None,  # (host, port) tuple
            'max_memory': None,  # in bytes
            'max_clients': 10000
        }
        
        if config:
            self.config.update(config)
        
        # Initialize core components
        self._data_store = DataStore()
        self._running = False
        self._threads = []
        
        # Initialize persistence
        self._aof = AOFManager(self._data_store, self.config['aof_file']) if self.config['aof_enabled'] else None
        self._rdb = RDBManager(self._data_store) if self.config['rdb_enabled'] else None
        
        # Initialize replication
        self._repl = ReplicationManager(self._data_store)
        if self.config['replicaof']:
            host, port = self.config['replicaof']
            self._repl.configure_as_replica(host, port)
        
        # Initialize monitoring
        self._monitor = Monitoring()
        
        # Initialize security
        self._authenticator = Authenticator(self.config['requirepass'])
        self._acl = ACLManager()
        self._tls = None
        if self.config['tls_enabled']:
            self._tls = TLSWrapper(
                certfile=self.config['tls_cert_file'],
                keyfile=self.config['tls_key_file'],
                ca_certs=self.config['tls_ca_cert_file'],
                require_client_cert=self.config['tls_require_client_cert']
            )
        
        # Initialize command handler with all dependencies
        self._handler = CommandHandler(
            self._data_store,
            aof_manager=self._aof,
            rdb_manager=self._rdb,
            replication_manager=self._repl,
            monitor=self._monitor,
            authenticator=self._authenticator,
            acl_manager=self._acl
        )
        
        # Background tasks
        self._cleaner_thread = threading.Thread(
            target=self._clean_expired_keys,
            daemon=True
        )
        self._persistence_thread = threading.Thread(
            target=self._periodic_persistence,
            daemon=True
        )
        self._replication_thread = threading.Thread(
            target=self._replication_sync,
            daemon=True
        )

    def start(self) -> None:
        """Start the Redis server with all components"""
        print(f"Starting Redis server on {self.config['host']}:{self.config['port']}")
        
        # Load data from persistence
        self._load_data()
        
        # Start background threads
        self._running = True
        self._cleaner_thread.start()
        self._persistence_thread.start()
        
        if self.config['replicaof']:
            self._replication_thread.start()
        
        # Main server loop
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.config['host'], self.config['port']))
            s.listen(self.config['max_clients'])
            
            while self._running:
                try:
                    conn, addr = s.accept()
                    self._monitor.client_connected()
                    
                    thread = threading.Thread(
                        target=self._handle_connection,
                        args=(conn, addr)
                    )
                    self._threads.append(thread)
                    thread.start()
                except OSError:
                    break  # Server is shutting down

    def stop(self) -> None:
        """Stop the server gracefully"""
        print("Shutting down server...")
        self._running = False
        
        # Unblock accept() call
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.config['host'], self.config['port']))
        except:
            pass
        
        # Save data before shutdown
        self._save_data()
        
        # Clean up threads
        for thread in self._threads:
            thread.join()
        
        # Close persistence
        if self._aof:
            self._aof.close()
        
        print("Server stopped")

    def _handle_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Handle a client connection with all security checks"""
        try:
            # Wrap with TLS if enabled
            if self._tls:
                try:
                    conn = self._tls.wrap_socket(conn)
                    if not self._tls.validate_client_cert(conn):
                        conn.sendall(b"-ERR Invalid client certificate\r\n")
                        return
                except ssl.SSLError as e:
                    print(f"TLS handshake failed with {addr}: {str(e)}")
                    return
            
            # Authentication state
            authenticated = not self._authenticator.require_password()
            username = None
            
            with conn:
                while self._running:
                    try:
                        data = conn.recv(1024)
                        if not data:
                            break
                        
                        # Parse command
                        try:
                            parts = data.decode().strip().split()
                            if not parts:
                                continue
                            
                            command = parts[0].upper()
                            args = parts[1:]
                            
                            # Handle authentication
                            if not authenticated and command not in ('AUTH', 'HELLO', 'QUIT'):
                                conn.sendall(b"-NOAUTH Authentication required\r\n")
                                continue
                            
                            # Record command start time for monitoring
                            start_time = time.monotonic()
                            
                            # Handle command
                            response = self._handler.handle_command(command, args, username)
                            
                            # Record command metrics
                            latency = (time.monotonic() - start_time) * 1_000_000  # microseconds
                            self._monitor.record_command(command, latency)
                            
                            # Handle authentication success
                            if command == 'AUTH':
                                if len(args) == 1:  # Password only
                                    if self._authenticator.authenticate(None, args[0]):
                                        authenticated = True
                                        response = "+OK\r\n"
                                elif len(args) == 2:  # Username and password
                                    if self._authenticator.authenticate(args[0], args[1]):
                                        authenticated = True
                                        username = args[0]
                                        response = "+OK\r\n"
                            
                            conn.sendall(response.encode())
                        except UnicodeDecodeError:
                            conn.sendall(b"-ERR Invalid UTF-8\r\n")
                        except Exception as e:
                            conn.sendall(f"-ERR {str(e)}\r\n".encode())
                    except (ConnectionError, OSError):
                        break
        finally:
            self._monitor.client_disconnected()

    def _load_data(self) -> None:
        """Load data from persistence files"""
        print("Loading data from persistence...")
        
        # Try loading from AOF first if enabled
        if self._aof and Path(self.config['aof_file']).exists():
            try:
                self._aof.load()
                print(f"Loaded data from AOF file: {self.config['aof_file']}")
                return
            except Exception as e:
                print(f"Failed to load AOF: {str(e)}")
        
        # Fall back to RDB if enabled
        if self._rdb and Path(self.config['rdb_file']).exists():
            try:
                self._rdb.load(self.config['rdb_file'])
                print(f"Loaded data from RDB file: {self.config['rdb_file']}")
            except Exception as e:
                print(f"Failed to load RDB: {str(e)}")

    def _save_data(self) -> None:
        """Save data to persistence files"""
        print("Saving data to persistence...")
        
        # Save to RDB if enabled
        if self._rdb:
            try:
                self._rdb.save(self.config['rdb_file'])
                print(f"Saved data to RDB file: {self.config['rdb_file']}")
            except Exception as e:
                print(f"Failed to save RDB: {str(e)}")
        
        # Rewrite AOF if enabled
        if self._aof:
            try:
                self._aof.rewrite()
                print(f"Rewrote AOF file: {self.config['aof_file']}")
            except Exception as e:
                print(f"Failed to rewrite AOF: {str(e)}")

    def _clean_expired_keys(self) -> None:
        """Background task to clean expired keys"""
        while self._running:
            time.sleep(1)
            try:
                self._data_store._clean_expired()
            except Exception as e:
                print(f"Error cleaning expired keys: {str(e)}")

    def _periodic_persistence(self) -> None:
        """Background task for periodic persistence"""
        last_save = time.time()
        changes_since_last = 0
        
        while self._running:
            time.sleep(1)
            
            # Check if we need to save
            if self._rdb and self._rdb.needs_save(changes_since_last):
                try:
                    self._rdb.save(self.config['rdb_file'])
                    last_save = time.time()
                    changes_since_last = 0
                    print("Periodic RDB save completed")
                except Exception as e:
                    print(f"Periodic save failed: {str(e)}")
            
            # Count changes (simplified - in real Redis this would track actual changes)
            changes_since_last += 1

    def _replication_sync(self) -> None:
        """Background task for replica synchronization"""
        while self._running:
            try:
                if self.config['replicaof'] and not self._repl.sync_with_master():
                    print("Replication sync failed, retrying in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                print(f"Replication error: {str(e)}")
                time.sleep(5)

    def get_info(self) -> Dict[str, str]:
        """Get server information for INFO command"""
        return self._monitor.get_info_sections()