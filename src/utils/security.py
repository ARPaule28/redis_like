import os
import ssl
import hashlib
import threading
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum, auto

class ACLPermission(Enum):
    """ACL permission flags"""
    READ = auto()
    WRITE = auto()
    ADMIN = auto()
    ALL = auto()

@dataclass
class ACLRule:
    """ACL rule definition"""
    patterns: Set[str]
    permissions: Set[ACLPermission]
    allowed_commands: Set[str]
    denied_commands: Set[str]

class Authenticator:
    """Handles client authentication"""
    def __init__(self, requirepass: Optional[str] = None):
        self._requirepass = requirepass
        self._users: Dict[str, str] = {}  # username -> password hash
        self._lock = threading.Lock()
        
        if requirepass:
            self.add_user('default', requirepass)

    def add_user(self, username: str, password: str) -> None:
        """Add a new user with password"""
        with self._lock:
            self._users[username] = self._hash_password(password)

    def authenticate(self, username: Optional[str], password: str) -> bool:
        """Authenticate a client"""
        if not self._requirepass and not self._users:
            return True  # No authentication required
        
        with self._lock:
            if username:
                # Username/password auth
                stored_hash = self._users.get(username)
                if not stored_hash:
                    return False
                return self._verify_password(password, stored_hash)
            else:
                # Legacy auth with default user
                if 'default' not in self._users:
                    return False
                return self._verify_password(password, self._users['default'])

    def _hash_password(self, password: str) -> str:
        """Generate secure password hash"""
        salt = os.urandom(16)
        key = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt,
            100000
        )
        return f"pbkdf2:sha256:100000${salt.hex()}${key.hex()}"

    def _verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify password against stored hash"""
        try:
            method, algo, iterations, salt, key = stored_hash.split('$')
            if method != 'pbkdf2' or algo != 'sha256':
                return False
            
            new_key = hashlib.pbkdf2_hmac(
                algo,
                password.encode('utf-8'),
                bytes.fromhex(salt),
                int(iterations)
            )
            return new_key.hex() == key
        except (ValueError, AttributeError):
            return False

    def require_password(self) -> bool:
        """Check if password authentication is required"""
        return self._requirepass is not None or bool(self._users)

class ACLManager:
    """Access Control List manager"""
    def __init__(self):
        self._rules: Dict[str, ACLRule] = {}  # username -> ACLRule
        self._default_permissions = {
            ACLPermission.READ,
            ACLPermission.WRITE
        }
        self._lock = threading.Lock()

    def add_rule(self, username: str, rule: ACLRule) -> None:
        """Add an ACL rule for a user"""
        with self._lock:
            self._rules[username] = rule

    def check_permission(self, username: Optional[str], 
                        command: str, 
                        key: Optional[str] = None) -> bool:
        """Check if user has permission to execute command"""
        if not username or username not in self._rules:
            # Use default permissions
            required_perms = self._command_permissions(command)
            return required_perms.issubset(self._default_permissions)
        
        with self._lock:
            rule = self._rules.get(username)
            if not rule:
                return False
            
            # Check command restrictions
            if rule.denied_commands and command in rule.denied_commands:
                return False
            if rule.allowed_commands and command not in rule.allowed_commands:
                return False
            
            # Check key patterns if provided
            if key and rule.patterns:
                if not any(self._match_pattern(key, pattern) 
                          for pattern in rule.patterns):
                    return False
            
            # Check permissions
            required_perms = self._command_permissions(command)
            return required_perms.issubset(rule.permissions)

    def _command_permissions(self, command: str) -> Set[ACLPermission]:
        """Determine required permissions for a command"""
        command = command.upper()
        if command in ('AUTH', 'PING', 'INFO'):
            return set()
        elif command in ('CONFIG', 'SHUTDOWN', 'SAVE'):
            return {ACLPermission.ADMIN}
        elif command in ('GET', 'HGET', 'LRANGE', 'SMEMBERS'):
            return {ACLPermission.READ}
        elif command in ('SET', 'HSET', 'LPUSH', 'SADD'):
            return {ACLPermission.WRITE}
        return {ACLPermission.ALL}

    def _match_pattern(self, key: str, pattern: str) -> bool:
        """Check if key matches the pattern (simplified glob-style)"""
        if pattern == '*':
            return True
        if pattern.startswith('^') and pattern.endswith('$'):
            return key == pattern[1:-1]
        if pattern.startswith('^'):
            return key.startswith(pattern[1:])
        if pattern.endswith('$'):
            return key.endswith(pattern[:-1])
        return pattern in key

class TLSWrapper:
    """Handles TLS/SSL encryption for connections"""
    def __init__(self, certfile: str, keyfile: str, 
                 ca_certs: Optional[str] = None,
                 require_client_cert: bool = False):
        self._certfile = certfile
        self._keyfile = keyfile
        self._ca_certs = ca_certs
        self._require_client_cert = require_client_cert
        self._context = self._create_context()

    def _create_context(self) -> ssl.SSLContext:
        """Create SSL context with configured settings"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(
            certfile=self._certfile,
            keyfile=self._keyfile
        )
        
        if self._ca_certs:
            context.load_verify_locations(cafile=self._ca_certs)
            context.verify_mode = (
                ssl.CERT_REQUIRED if self._require_client_cert 
                else ssl.CERT_OPTIONAL
            )
        else:
            context.verify_mode = ssl.CERT_NONE
        
        context.set_ciphers('HIGH:!aNULL:!eNULL:!MD5')
        context.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        return context

    def wrap_socket(self, sock: socket.socket) -> ssl.SSLSocket:
        """Wrap a socket with TLS encryption"""
        return self._context.wrap_socket(
            sock,
            server_side=True,
            do_handshake_on_connect=True
        )

    def validate_client_cert(self, sock: ssl.SSLSocket) -> bool:
        """Validate client certificate if required"""
        if not self._require_client_cert:
            return True
        
        cert = sock.getpeercert()
        if not cert:
            return False
        
        # Add additional validation logic here as needed
        return True