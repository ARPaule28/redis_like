class RedisError(Exception):
    """Base Redis-like exception"""
    pass

class KeyNotFoundError(RedisError):
    """Key does not exist"""
    def __init__(self, key: str):
        super().__init__(f"Key '{key}' not found")

class WrongTypeError(RedisError):
    """Operation against wrong data type"""
    def __init__(self, expected: str, actual: str):
        super().__init__(f"Wrong type, expected {expected}, got {actual}")

class InvalidCommandError(RedisError):
    """Invalid command syntax"""
    def __init__(self, command: str):
        super().__init__(f"Invalid command: {command}")

class OutOfRangeError(RedisError):
    """Index out of range"""
    pass

class SyntaxError(RedisError):
    """Command syntax error"""
    pass

class GeoError(RedisError):
    """Geospatial operation error"""
    pass

class VectorError(RedisError):
    """Vector operation error"""
    pass