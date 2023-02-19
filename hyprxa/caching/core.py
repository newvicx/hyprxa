import functools
import hashlib
import inspect
import logging
from types import FunctionType, MethodType
from typing import Any, Callable, Generator, List, Tuple

from hyprxa.caching.base import BaseCachedFunction
from hyprxa.caching.exceptions import (
    CacheKeyNotFoundError,
    UnhashableParamError,
    UnhashableTypeError,
    UnserializableReturnValueError
)
from hyprxa.caching.hashing import update_hash
from hyprxa.util.caching import CacheType



_LOGGER = logging.getLogger("hyprxa.caching")


def create_cache_wrapper(cached_func: BaseCachedFunction) -> Callable[..., Any]:
    """Create a wrapper for a CachedFunction."""
    func = cached_func.func
    function_key = make_function_key(cached_func.cache_type, cached_func.func)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """This function wrapper will only call the underlying function in
        the case of a cache miss.
        """
        cache = cached_func.get_function_cache(function_key=function_key)
        with cache.execution_lock:
            rw = read_write_cached_value(cached_func, function_key, func, *args, **kwargs)
            try:
                next(rw)
            except StopIteration as e:
                return e.value
            else:
                return_value = func(*args, **kwargs)
                try:
                    rw.send(return_value)
                except StopIteration:
                    pass
                return return_value

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        """This function wrapper will only call the underlying function in
        the case of a cache miss.
        """
        cache = cached_func.get_function_cache(function_key=function_key)
        async with cache.execution_lock:
            rw = read_write_cached_value(cached_func, function_key, func, *args, **kwargs)
            try:
                next(rw)
            except StopIteration as e:
                return e.value
            else:
                return_value = await func(*args, **kwargs)
                try:
                    rw.send(return_value)
                except StopIteration:
                    pass
                return return_value

    if inspect.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


def read_write_cached_value(
    cached_func: BaseCachedFunction,
    function_key: str,
    func: Callable[[Any], Any],
    *args: Any,
    **kwargs: Any
) -> Generator[None, Any, Any]:
    """A common workflow for reading/writing from/to a cache in both a sync and
    async wrapper.

    The actual function call is handled outside this generator.

    Raises:
        UnserializableReturnValueError: When a return value from a function
            cannot be serialized with pickle (only applies to memo caches).
    """
    # Retrieve the function's cache object. We must do this inside the
    # wrapped function, because caches can be invalidated at any time.
    cache = cached_func.get_function_cache(function_key)
    value_key = make_value_key(cached_func.cache_type, func, *args, **kwargs)
    try:
        result = cache.read_result(value_key)
        _LOGGER.debug("Cache hit: %s", func)

        return result

    except CacheKeyNotFoundError:
        _LOGGER.debug("Cache miss: %s", func)

        return_value = yield

        try:
            cache.write_result(value_key, return_value)
        except TypeError:
            raise UnserializableReturnValueError(
                return_value=return_value, func=cached_func.func
            )

def make_value_key(
    cache_type: CacheType,
    func: FunctionType,
    *args: Any,
    **kwargs: Any
) -> str:
    """Create the key for a value within a cache.
    
    This key is generated from the function's arguments. All arguments
    will be hashed, except for `self`, `cls` and those named with a leading "_".
    
    Raises:
        UnhashableParamError: When one of the arguments is not hashable. This can
            can be avoided by attaching a leading underscore (_) to the argument
            in the function definition.
    """
    arg_pairs: List[Tuple[str | None, Any]] = []
    for arg_idx in range(len(args)):
        arg_name = get_positional_arg_name(func, arg_idx)
        arg_pairs.append((arg_name, args[arg_idx]))

    for kw_name, kw_val in kwargs.items():
        # **kwargs ordering is preserved, per PEP 468
        # https://www.python.org/dev/peps/pep-0468/, so this iteration is
        # deterministic.
        arg_pairs.append((kw_name, kw_val))

    # Create the hash from each arg value, except for those args whose name
    # starts with "_".
    args_hasher = hashlib.new("md5")

    if isinstance(func, MethodType):
        arg_pairs = arg_pairs[1:]

    for arg_name, arg_value in arg_pairs:
        if arg_name is not None and arg_name.startswith("_"):
            _LOGGER.debug("Not hashing %s because it starts with _", arg_name)
            continue
        try:
            update_hash(
                (arg_name, arg_value),
                hasher=args_hasher,
                cache_type=cache_type,
            )
        except UnhashableTypeError as e:
            raise UnhashableParamError(func, arg_name, arg_value, e)

    value_key = args_hasher.hexdigest()
    _LOGGER.debug("Cache key: %s", value_key)

    return value_key


def make_function_key(cache_type: CacheType, func: FunctionType) -> str:
    """Create the unique key for a function's cache.
    
    A function's key is stable across multiple calls, and changes when the
    function's source code changes.
    """
    func_hasher = hashlib.new("md5")

    # Include the function's __module__ and __qualname__ strings in the hash.
    # This means that two identical functions in different modules
    # will not share a hash; it also means that two identical nested
    # functions in the same module will not share a hash.
    update_hash(
        (func.__module__, func.__qualname__),
        hasher=func_hasher,
        cache_type=cache_type,
    )

    # Include the function's source code in its hash. If the source code can't
    # be retrieved, fall back to the function's bytecode instead.
    source_code: str | bytes
    try:
        source_code = inspect.getsource(func)
    except OSError as e:
        _LOGGER.debug(
            "Failed to retrieve function's source code when building its key; "
            "falling back to bytecode. err={0}",
            e,
        )
        source_code = func.__code__.co_code

    update_hash(
        source_code,
        hasher=func_hasher,
        cache_type=cache_type,
    )

    cache_key = func_hasher.hexdigest()
    return cache_key


def get_positional_arg_name(func: FunctionType, arg_index: int) -> str | None:
    """Return the name of a function's positional argument.
    
    If arg_index is out of range, or refers to a parameter that is not a
    named positional argument (e.g. an *args, **kwargs, or keyword-only param),
    return None instead.
    """
    if arg_index < 0:
        return None

    params: List[inspect.Parameter] = list(inspect.signature(func).parameters.values())
    if arg_index >= len(params):
        return None

    if params[arg_index].kind in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    ):
        return params[arg_index].name

    return None


def create_cached_func_wrapper(
    func: FunctionType,
    cached_func: BaseCachedFunction
) -> Callable[..., Any]:
    """Implements common plumbing for `@memo` and `@singleton` api's."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Wraps a sync function to be cached."""
        wrapped = create_cache_wrapper(cached_func)
        return wrapped(*args, **kwargs)

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        """Wraps an async function to be cached."""
        wrapped = create_cache_wrapper(cached_func)
        return await wrapped(*args, **kwargs)

    if inspect.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


def clear_cached_func(cached_func: BaseCachedFunction) -> Callable[[], None]:
    """Calls the `clear` method on a cache."""
    def wrapper():
        function_key = make_function_key(cached_func.cache_type, cached_func.func)
        cache = cached_func.get_function_cache(function_key)
        cache.clear()
    return wrapper


def invalidate_cached_value(cached_func: BaseCachedFunction) -> Callable[..., None]:
    """Calls the `invalidate` method on a cache."""
    def wrapper(*args: Any, **kwargs: Any) -> None:
        function_key = make_function_key(cached_func.cache_type, cached_func.func)
        cache = cached_func.get_function_cache(function_key)
        value_key = make_value_key(cached_func.cache_type, cached_func.func, *args, **kwargs)
        cache.invalidate(value_key)
    return wrapper