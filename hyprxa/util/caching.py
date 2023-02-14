import re
from enum import Enum



class CacheType(str, Enum):
    """The available cache types for function caches."""
    MEMO = "memo"
    SINGLETON = "singleton"


class NoResult:
    """Placeholder class for return values when None is meaningful."""


def is_type(obj: object, fqn_type_pattern: str | re.Pattern[str]) -> bool:
    """Check type without importing expensive modules.
    
    Args
        obj: The object to type-check.
        fqn_type_pattern: The fully-qualified type string or a regular expression.
            Regexes should start with `^` and end with `$`.
    
    Examples:
    To check whether something is a Matplotlib Figure without importing
    matplotlib, use:
    >>> is_type(foo, 'matplotlib.figure.Figure')
    """
    fqn_type = get_fqn_type(obj)
    if isinstance(fqn_type_pattern, str):
        return fqn_type_pattern == fqn_type
    else:
        return fqn_type_pattern.match(fqn_type) is not None


def get_fqn(the_type: type) -> str:
    """Get module.type_name for a given type."""
    return f"{the_type.__module__}.{the_type.__qualname__}"


def get_fqn_type(obj: object) -> str:
    """Get module.type_name for a given object."""
    return get_fqn(type(obj))


def repr_(cls) -> str:
    """Represent a class as '{classname}({args})'."""
    classname = cls.__class__.__name__
    args = ", ".join([f"{k}={repr(v)}" for (k, v) in cls.__dict__.items()])
    return f"{classname}({args})"


def get_cached_func_name(func) -> str:
    """Return the name of the given function."""
    if hasattr(func, "__name__"):
        return "{}".format(func.__name__)
    elif hasattr(type(func), "__name__"):
        return "{}".format(type(func).__name__)
    return "{}".format(type(func))


def get_return_value_type(return_value) -> str:
    """Get the type of an object as a string."""
    if hasattr(return_value, "__module__") and hasattr(type(return_value), "__name__"):
        return "{}.{}".format(return_value.__module__, type(return_value).__name__)
    return get_cached_func_name(return_value)