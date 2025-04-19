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