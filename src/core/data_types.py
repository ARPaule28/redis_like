from enum import Enum

class DataType(Enum):
    STRING = 'string'
    LIST = 'list'
    SET = 'set'
    HASH = 'hash'
    ZSET = 'zset'
    STREAM = 'stream'
    BITMAP = 'bitmap'
    GEO = 'geo'
    VECTOR = 'vector'
    TIMESERIES = 'timeseries'

class CommandCategory(Enum):
    KEY = 'key'
    STRING = 'string'
    LIST = 'list'
    SET = 'set'
    HASH = 'hash'
    SORTED_SET = 'sorted_set'
    STREAM = 'stream'
    BITMAP = 'bitmap'
    GEO = 'geo'
    VECTOR = 'vector'
    ADMIN = 'admin'