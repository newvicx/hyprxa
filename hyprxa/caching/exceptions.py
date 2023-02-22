from types import FunctionType
from typing import Any

from hyprxa.util.caching import (
    get_cached_func_name,
    get_fqn_type,
    get_return_value_type
)
from hyprxa.util.formatting import format_docstring
from hyprxa._exceptions import HyprxaError



class CachingException(HyprxaError):
    """Base cache exception that all exceptions raised by cache decorators
    derive from.
    """


class UnhashableTypeError(CachingException):
    """Internal exception raised when a function argument is not hashable."""


class CacheKeyNotFoundError(CachingException):
    """The hash result of the inputs does not match the key in a cache result.
    
    This normally leads to a cache miss and is not propagated.
    """


class CacheError(CachingException):
    """Raised by the `memo` decorator when an object cannot be pickled or `memo`
    cannot read/write from/to the persisting backend.
    """


class UnhashableParamError(CachingException):
    """Raised when an input value to a caching function is not hashable.
    
    This can be avoided by attaching a leading underscore (_) to the argument.
    The argument will be skipped when calculating the cache key.
    """
    def __init__(
        self,
        func: FunctionType,
        arg_name: str | None,
        arg_value: Any,
        orig_exc: BaseException,
    ):
        msg = self._create_message(func, arg_name, arg_value)
        super().__init__(msg)
        self.with_traceback(orig_exc.__traceback__)

    @staticmethod
    def _create_message(
        func: FunctionType,
        arg_name: str | None,
        arg_value: Any,
    ) -> str:
        arg_name_str = arg_name if arg_name is not None else "(unnamed)"
        arg_type = get_fqn_type(arg_value)
        func_name = func.__name__
        arg_replacement_name = f"_{arg_name}" if arg_name is not None else "_arg"

        return format_docstring("""Cannot hash argument '{}' (of type {}) in
            '{}'. To address this, you can force this argument to be ignored by
            adding a leading underscore to the arguments name in the function
            signature (eg. '{}').""".format(
                arg_name_str,
                arg_type,
                func_name,
                arg_replacement_name
            )
        )


class UnserializableReturnValueError(CachingException):
    """Raised when a return value from a function cannot be serialized with pickle."""
    def __init__(self, func: FunctionType, return_value: FunctionType):
        msg = self._create_message(func, return_value)
        super().__init__(msg)
    
    def _create_message(self, func, return_value) -> str:
        return format_docstring("""Cannot serialize the return value of type
            '{}' in '{}'. 'memo' uses pickle to serialize the functions return
            value and safely store it in cache without mutating the original
            object. Please convert the return value to a pickle-serializable
            type. If you want to cache unserializable objects such as database
            connections or HTTP sessions, use 'singleton' instead.""".format(
                get_return_value_type(return_value),
                get_cached_func_name(func)
            )
        )