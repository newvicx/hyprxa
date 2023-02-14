from .caches import Caches
from .exceptions import (
    CachingException,
    CacheError,
    UnhashableParamError,
    UnserializableReturnValueError
)
from .memo import (
    memo,
    set_cache_dir,
    set_default_backend,
    set_memcached_client,
    set_redis_client
)
from .singleton import singleton, iter_singletons



__all__ = [
    "Caches",
    "CachingException",
    "CacheError",
    "UnhashableParamError",
    "UnserializableReturnValueError",
    "memo",
    "set_cache_dir",
    "set_default_backend",
    "set_memcached_client",
    "set_redis_client",
    "iter_singletons",
    "singleton",
]