import asyncio
import inspect
import logging
import pickle
import threading
import types
from datetime import timedelta
from typing import Any, Callable, TypeVar

from pymemcache import PooledClient as Memcached

from hyprxa.caching.base import (
    BaseCache,
    BaseCacheCollection,
    BaseCachedFunction
)
from hyprxa.caching.core import (
    create_cached_func_wrapper,
    invalidate_cached_value
)
from hyprxa.caching.exceptions import CacheError, CacheKeyNotFoundError
from hyprxa.settings import CACHE_SETTINGS, MEMCACHED_SETTINGS
from hyprxa.util.caching import CacheType



_LOGGER = logging.getLogger("hyprxa.caching.memo")


class MemoCache(BaseCache):
    """Cache for memoized functions caches backed by Memcached."""
    def __init__(
        self,
        key: str,
        ttl: float | None,
        display_name: str
    ) -> None:
        self.key = key
        self.display_name = display_name
        self._ttl = ttl

    def read_result(self, value_key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        pickled_entry = self._read_from_memcached(value_key)

        try:
            return pickle.loads(pickled_entry)
        except pickle.UnpicklingError as e:
            raise CacheError(f"Failed to unpickle {value_key}.") from e

    def write_result(self, value_key: str, value: Any) -> None:
        """Write a value and associated messages to the cache.
        
        The value must be pickleable.
        """
        try:
            pickled_entry = pickle.dumps(value)
        except pickle.PicklingError as e:
            raise CacheError(f"Failed to pickle {value_key}.") from e

        self._write_to_memcached(value_key, pickled_entry)

    def clear(self) -> None:
        """Clear all in-memory values from this function cache."""
        raise NotImplementedError()

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        self._remove_from_memcached(value_key)

    def _read_from_memcached(self, value_key: str) -> bytes:
        """Read a cached value from the Memcached server."""
        key = self._get_memcached_key(value_key)
        try:
            client = memo.get_client()
            with memo.limiter:
                value = client.get(key)
        except Exception as e:
            _LOGGER.warning("Unable to read from memcached cache", exc_info=True)
            raise CacheError("Unable to read from cache.") from e
        if value is None:
            raise CacheKeyNotFoundError("Key not found in memcached cache.")
        _LOGGER.debug("Memcached cache second stage HIT: %s", key)
        return value

    def _write_to_memcached(self, value_key: str, pickled_value: bytes) -> None:
        """Write a value to the Memcached server."""
        key = self._get_memcached_key(value_key)
        try:
            client = memo.get_client()
            with memo.limiter:
                value = client.set(key, pickled_value, expire=self._ttl)
        except Exception as e:
            _LOGGER.warning("Unable to write to memcached cache", exc_info=True)
            raise CacheError("Unable to write to cache.") from e
        if not value:
            raise CacheError("Unable to write to cache.")

    def _remove_from_memcached(self, value_key: str) -> None:
        """Delete a cache key from Memcached. If the cache key does not exist,
        return silently.
        """
        key = self._get_memcached_key(value_key)
        try:
            client = memo.get_client()
            with memo.limiter:
                deleted = client.delete(key)
        except Exception:
            _LOGGER.warning("Unable to delete from memcached", exc_info=True)
        _LOGGER.debug("Memcached delete result %s: %s", key, deleted)

    def _get_memcached_key(self, value_key: str) -> str:
        """Get the memcached key from the function cache key and value key."""
        return f"{self.key}-{value_key}"


class MemoizedFunction(BaseCachedFunction):
    """Implements the `BaseCachedFunction` protocol for `@memo`"""
    def __init__(
        self,
        func: types.FunctionType,
        ttl: float | None
    ):
        super().__init__(func)
        self.ttl = ttl

    @property
    def cache_type(self) -> CacheType:
        return CacheType.MEMO

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> MemoCache:
        return memo_cache_collection.get_cache(
            func=self.func,
            key=function_key,
            ttl=self.ttl,
            display_name=self.display_name
        )


class MemoCacheCollection(BaseCacheCollection):
    """Manages all MemoCache instances"""
    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        display_name: str,
        ttl: int | float | None
    ) -> MemoCache:
        """Return the cache for the given key.

        If it doesn't exist, create a new one with the given params.
        """

        # Get the existing cache, if it exists
        with self._caches_lock:
            cache = self._function_caches.get(key)
            if cache is not None:
                assert cache._execution_lock is not None
                return cache

            # Create a new cache object and put it in our dict
            _LOGGER.debug(
                "Creating new MemoCache (key=%s, ttl=%s)",
                key,
                ttl,
            )
            cache = MemoCache(
                key=key,
                ttl=ttl,
                display_name=display_name
            )
            
            if inspect.iscoroutinefunction(func):
                lock = asyncio.Lock()
            else:
                lock = threading.Lock()
            cache.set_lock(lock)
            
            self._function_caches[key] = cache
            return cache


class MemoAPI:
    """Implements the public memo API: the `@memo` decorator."""
    F = TypeVar("F", bound=Callable[..., Any])
    client: Memcached = None
    limiter: threading.Semaphore = None
    _lock: threading.Lock = threading.Lock()

    def __call__(
        self,
        func: F | None = None,
        *,
        ttl: float | timedelta | None = None
    ):
        """Function decorator to memoize function executions.
        
        The memo API uses Memcached as its backend for storing cached values.
        You must have a Memcached server set up to use this API.

        Memoized data is stored in "pickled" form, which means that the return
        value of a memoized function must be pickleable.
        
        Each caller of a memoized function gets its own copy of the cached data.

        This decorator works with both sync and async functions.
        
        Args:
            func: The function to memoize. This hashes the function's source code.
            ttl: The maximum number of seconds to keep an entry in the cache.
                `None` for no expiry.

        Examples:
        >>> @memo
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered.
        >>> d1 = fetch_and_clean_data(DATA_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value. This means that now d1 equals d2
        >>> d2 = fetch_and_clean_data(DATA_URL_1)
        >>> # This is a different URL, so the function executes.
        >>> d3 = fetch_and_clean_data(DATA_URL_2)
        
        By default, all parameters to a memoized function must be hashable.
        Any parameter whose name begins with `_` will not be hashed. You can use
        this as an "escape hatch" for parameters that are not hashable...
        >>> @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> connection = make_database_connection()
        >>> # Actually executes the function, since this is the first time it was
        >>> # encountered.
        >>> d1 = fetch_and_clean_data(connection, num_rows=10)
        >>> # Does not execute the function. Instead, returns its previously computed
        >>> # value - even though the _database_connection parameter was different
        >>> # in both calls.
        >>> another_connection = make_database_connection()
        >>> d2 = fetch_and_clean_data(another_connection, num_rows=10)

        Memo caches cannot be procedurally cleared like singleton caches. However,
        a cached value can be procedurally invalidated by calling the `invalidate`
        method. This will delete the cached value from the server...
        >>> @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> # Invalidate a cached value
        >>> fetch_and_clean_data.invalidate(another_connection, num_rows=10)

        For HTTP cache-control use cases you can access the TTL property of
        memoize function directly...
        >>> ttl = fetch_and_clean_data.ttl
        """
        if isinstance(ttl, timedelta):
            ttl_seconds = ttl.total_seconds()
        elif ttl is None:
            ttl_seconds = CACHE_SETTINGS.ttl
        else:
            ttl_seconds = ttl
        
        if func is None:
            def decorator(f):
                cached_func = MemoizedFunction(
                    f,
                    ttl=ttl_seconds
                )
                wrapper = create_cached_func_wrapper(f, cached_func)
                wrapper.invalidate = invalidate_cached_value(cached_func)
                wrapper.ttl = ttl_seconds
                # Attach the function signature to the wrapper so the memoized
                # function can be used as a dependency.
                wrapper.__signature__ = inspect.signature(f)
                return wrapper
            return decorator
        
        else:
            cached_func = MemoizedFunction(func, ttl=ttl_seconds)
            wrapper = create_cached_func_wrapper(func, cached_func)
            wrapper.invalidate = invalidate_cached_value(cached_func)
            wrapper.ttl = ttl_seconds
            wrapper.__signature__ = inspect.signature(func)
            return wrapper

    @classmethod
    def get_client(cls) -> Memcached:
        """Get a memcached client."""
        with cls._lock:
            if cls.client is not None:
                assert cls.limiter is not None
                return cls.client
            assert cls.limiter is None
            _LOGGER.debug("Creating new memcached client")
            client = MEMCACHED_SETTINGS.get_client()
            cls.client = client
            # Limiter prevents more than `max_pool_size` threads from concurrent
            # access which would lead to a `RuntimeError`.
            cls.limiter = MEMCACHED_SETTINGS.get_limiter()
            return client



memo_cache_collection = MemoCacheCollection()
memo = MemoAPI()