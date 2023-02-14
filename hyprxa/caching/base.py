import asyncio
import threading
from types import FunctionType
from typing import Any, Callable, Dict, Union

from hyprxa.util.caching import CacheType



class BaseCache:
    """Standard function cache interface."""
    def __init__(self) -> None:
        self._execution_lock: Union[threading.Lock, asyncio.Lock] = None

    @property
    def execution_lock(self) -> Union[threading.Lock, asyncio.Lock]:
        return self._execution_lock

    def read_result(self, value_key: str) -> Any:
        """Read a value and associated messages from the cache.
        
        Raises:
          CacheKeyNotFoundError: Raised if value_key is not in the cache.
        """
        raise NotImplementedError()

    def write_result(self, value_key: str, value: Any) -> None:
        """Write a value to the cache, overwriting any existing result that uses
        the value_key.

        Raises:
            CacheError: Raised if unable to write to cache.
        """
        raise NotImplementedError()

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        raise NotImplementedError()

    def clear(self) -> None:
        """Clear all values from this function cache."""
        raise NotImplementedError()

    def set_lock(self, lock: Union[threading.Lock, asyncio.Lock]) -> None:
        """Set the execution lock for the function cache.
        
        The execution lock enforces serial execution of the cached function to
        prevent simultaneous cache misses for the same inputs.
        """
        self._execution_lock = lock


class BaseCachedFunction:
    """Encapsulates data for a cached function instance."""
    def __init__(self, func: FunctionType):
        self.func = func

    @property
    def cache_type(self) -> CacheType:
        raise NotImplementedError

    def get_function_cache(self, function_key: str) -> BaseCache:
        """Get or create the function cache for the given key."""
        raise NotImplementedError


class BaseCacheCollection:
    """Manages all cache instances"""
    def __init__(self):
        self._caches_lock = threading.Lock()
        self._function_caches: Dict[str, BaseCache] = {}
    
    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        display_name: str,
        *args: Any,
        **kwargs: Any
    ) -> BaseCache:
        """Return a cache instance."""
        raise NotImplementedError()

    def clear_all(self) -> None:
        """Clear all caches."""
        raise NotImplementedError()