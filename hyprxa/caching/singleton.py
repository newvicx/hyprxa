import asyncio
import inspect
import logging
import threading
from collections.abc import Iterable
from typing import Any, Callable, Dict, TypeVar

from hyprxa.caching.base import (
    BaseCache,
    BaseCacheCollection,
    BaseCachedFunction
)
from hyprxa.caching.core import (
    clear_cached_func,
    create_cached_func_wrapper,
    invalidate_cached_value
)
from hyprxa.caching.exceptions import CacheKeyNotFoundError
from hyprxa.util.caching import CacheType



_LOGGER = logging.getLogger("hyprxa.caching.singleton")


class SingletonCache(BaseCache, Iterable[Any]):
    """Manages cached values for a single singleton function."""
    def __init__(self, key: str, display_name: str):
        self.key = key
        self.display_name = display_name
        self._mem_cache: Dict[str, Any] = {}
        self._mem_cache_lock = threading.Lock()

    def read_result(self, value_key: str) -> Any:
        """Read a value and associated messages from the cache.

        Raise `CacheKeyNotFoundError` if the value doesn't exist.
        """
        with self._mem_cache_lock:
            try:
                return self._mem_cache[value_key]
            except KeyError:
                raise CacheKeyNotFoundError()
    
    def write_result(self, value_key: str, value: Any) -> None:
        """Write a value and associated messages to the cache."""
        with self._mem_cache_lock:
            self._mem_cache[value_key] = value

    def clear(self) -> None:
        """Clear all values from this function cache."""
        with self._mem_cache_lock:
            self._mem_cache.clear()

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        with self._mem_cache_lock:
            self._mem_cache.pop(value_key, None)

    def __iter__(self) -> Iterable[Any]:
        with self._mem_cache_lock:
            for value in self._mem_cache.values():
                yield value


class SingletonFunction(BaseCachedFunction):
    """Implements the `CachedFunction` protocol for `@singleton`"""
    @property
    def cache_type(self) -> CacheType:
        """The cache type for this function."""
        return CacheType.SINGLETON

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> SingletonCache:
        """Get or create the function cache for the given key."""
        return singleton_cache_collection.get_cache(
            func=self.func,
            key=function_key,
            display_name=self.display_name
        )


class SingletonCaches(BaseCacheCollection, Iterable[Any]):
    """Manages all `SingletonCache` instances"""
    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        display_name: str
    ) -> SingletonCache:
        """Return the mem cache for the given key.
        
        If it doesn't exist, create a new one with the given params.
        """
        # Get the existing cache, if it exists
        with self._caches_lock:
            cache = self._function_caches.get(key)
            if cache is not None:
                assert cache._execution_lock is not None
                return cache

            # Create a new cache object and put it in our dict
            _LOGGER.debug("Creating new SingletonCache (key=%s)", key)
            cache = SingletonCache(
                key=key,
                display_name=display_name
            )
            if inspect.iscoroutinefunction(func):
                lock = asyncio.Lock()
            else:
                lock = threading.Lock()
            cache.set_lock(lock)
            self._function_caches[key] = cache
            return cache

    def clear_all(self) -> None:
        """Clear all singleton caches."""
        with self._caches_lock:
            self._function_caches = {}

    def __iter__(self) -> Iterable[Any]:
        with self._caches_lock:
            for cache in self._function_caches.values():
                with cache._mem_cache_lock:
                    for obj in cache._mem_cache.values():
                        yield obj


class SingletonAPI:
    """Implements the public singleton API: the `@singleton` decorator,
    and `singleton.clear()`.
    """
    F = TypeVar("F", bound=Callable[..., Any])

    def __call__(self, func: F | None = None):
        """Function decorator to store singleton objects.
        
        Each singleton object is shared across all threads in the application.
        Singleton objects must be thread-safe, because they can be accessed from
        multiple threads concurrently.

        This decorator works with both sync and async functions.
        
        Args:
            func: The function that creates the singleton. This hashes the
                function's source code.

        Examples:
        >>> @singleton
        ... def get_database_session(url):
        ...     # Create a database session object that points to the URL.
        ...     return session
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered...
        >>> s1 = get_database_session(SESSION_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value. This means that now the connection object in s1 is
        >>> # the same as in s2...
        >>> s2 = get_database_session(SESSION_URL_1)
        >>> assert id(s1) == id(s2)
        >>> # This is a different URL, so the function executes...
        >>> s3 = get_database_session(SESSION_URL_2)
        
        By default, all parameters to a singleton function must be hashable.
        Any parameter whose name begins with `_` will not be hashed. You can use
        this as an "escape hatch" for parameters that are not hashable...
        >>> @singleton
        ... def get_database_session(_sessionmaker, url):
        ...     # Create a database connection object that points to the URL.
        ...     return connection
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered...
        >>> s1 = get_database_session(SESSION_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value - even though the _sessionmaker parameter was
        >>> different in both calls...
        >>> s2 = get_database_session(create_sessionmaker(), DATA_URL_1)
        >>> assert id(s1) == id(s2)
        
        A singleton function's cache can be procedurally cleared...
        >>> @singleton
        ... def get_database_session(_sessionmaker, url):
        ...     # Create a database connection object that points to the URL.
        ...     return connection
        >>> # Clear all cached entries for this function.
        >>> get_database_session.clear()
        >>> # You can also clear all cached entries
        >>> singleton.clear()

        A singleton function's cache can be procedurally invalidated...
        >>> @singleton
        ... def get_database_session(_sessionmaker, url):
        ...     # Create a database connection object that points to the URL.
        ...     return connection
        >>> # Invalidate the session for a particular URL.
        >>> get_database_session.invalidate(_sessionmaker, url)

        You can iterate over all singletons in the cache...
        >>> for obj in singleton:
        ...     ...
        """
        if func is None:
            def decorator(f):
                cached_func = SingletonFunction(f)
                wrapper = create_cached_func_wrapper(f, cached_func)
                wrapper.clear = clear_cached_func(cached_func)
                wrapper.invalidate = invalidate_cached_value(cached_func)
                # Attach the function signature to the wrapper so the memoized
                # function can be used as a dependency.
                wrapper.__signature__ = inspect.signature(f)
                return wrapper
            return decorator
        
        else:
            cached_func = SingletonFunction(func)
            wrapper = create_cached_func_wrapper(func, cached_func)
            wrapper.clear = clear_cached_func(cached_func)
            wrapper.invalidate = invalidate_cached_value(cached_func)
            wrapper.__signature__ = inspect.signature(func)
            return wrapper

    @staticmethod
    def clear() -> None:
        """Clear all singleton caches."""
        singleton_cache_collection.clear_all()

    def __iter__(self) -> Iterable[Any]:
        for obj in singleton_cache_collection:
            yield obj


singleton_cache_collection = SingletonCaches()
singleton = SingletonAPI()