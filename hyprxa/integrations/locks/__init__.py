from enum import Enum

from .memcached import MemcachedLock
from .redis import RedisLock



__all__ = [
    "Locks",
    "MemcachedLock",
    "RedisLock",
]


class Locks(str, Enum):
    REDIS = "redis",
    MEMCACHED = "memchached"