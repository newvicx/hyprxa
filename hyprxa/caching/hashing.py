import collections
import dataclasses
import functools
import hashlib
import inspect
import io
import os
import sys
import tempfile
import threading
import unittest.mock
import weakref
from enum import Enum
from typing import Any, Dict, List, Pattern

from hyprxa.caching.exceptions import UnhashableTypeError
from hyprxa.util.caching import CacheType, NoResult, is_type, repr_



# Arbitrary item to denote where we found a cycle in a hashed object.
# This allows us to hash self-referencing lists, dictionaries, etc.
_CYCLE_PLACEHOLDER = b"newvicx-letsbuildkickassopensourcesoftware"


def update_hash(val: Any, hasher, cache_type: CacheType) -> None:
    """Updates a hashlib hasher with the hash of val.
    
    This is the main entrypoint to `hashing.py`.
    """
    ch = CacheFuncHasher(cache_type)
    ch.update(hasher, val)


class HashStack:
    """Stack of what has been hashed, for debug and circular reference detection.
    
    This internally keeps 1 stack per thread.
    
    Internally, this stores the ID of pushed objects rather than the objects
    themselves because otherwise the "in" operator inside __contains__ would
    fail for objects that don't return a boolean for "==" operator. For
    example, arr == 10 where arr is a NumPy array returns another NumPy array.
    
    This causes the "in" to crash since it expects a boolean.
    """
    def __init__(self):
        self._stack: collections.OrderedDict[int, List[Any]] = collections.OrderedDict()

    def __repr__(self) -> str:
        return repr_(self)

    def push(self, val: Any):
        self._stack[id(val)] = val

    def pop(self):
        self._stack.popitem()

    def __contains__(self, val: Any):
        return id(val) in self._stack


class HashStacks:
    """Stacks of what has been hashed, with at most 1 stack per thread."""
    def __init__(self):
        self._stacks: weakref.WeakKeyDictionary[
            threading.Thread, HashStack
        ] = weakref.WeakKeyDictionary()

    def __repr__(self) -> str:
        return repr_(self)

    @property
    def current(self) -> HashStack:
        current_thread = threading.current_thread()

        stack = self._stacks.get(current_thread, None)

        if stack is None:
            stack = HashStack()
            self._stacks[current_thread] = stack

        return stack


hash_stacks = HashStacks()


def int_to_bytes(i: int) -> bytes:
    num_bytes = (i.bit_length() + 8) // 8
    return i.to_bytes(num_bytes, "little", signed=True)


def key_(obj: Any | None) -> Any:
    """Return key for memoization."""
    if obj is None:
        return None

    def is_simple(obj):
        return (
            isinstance(obj, bytes)
            or isinstance(obj, bytearray)
            or isinstance(obj, str)
            or isinstance(obj, float)
            or isinstance(obj, int)
            or isinstance(obj, bool)
            or obj is None
        )

    if is_simple(obj):
        return obj

    if isinstance(obj, tuple):
        if all(map(is_simple, obj)):
            return obj

    if isinstance(obj, list):
        if all(map(is_simple, obj)):
            return ("__l", tuple(obj))

    if (
        inspect.isbuiltin(obj)
        or inspect.isroutine(obj)
        or inspect.iscode(obj)
    ):
        return id(obj)

    return NoResult


class CacheFuncHasher:
    """A hasher that can hash objects with cycles."""
    def __init__(self, cache_type: CacheType):
        self._hashes: Dict[Any, bytes] = {}

        # The number of the bytes in the hash.
        self.size = 0

        self.cache_type = cache_type

    def __repr__(self) -> str:
        return repr_(self)

    def to_bytes(self, obj: Any) -> bytes:
        """Add memoization to _to_bytes and protect against cycles in data structures."""
        tname = type(obj).__qualname__.encode()
        key = (tname, key_(obj))

        # Memoize if possible.
        if key[1] is not NoResult:
            if key in self._hashes:
                return self._hashes[key]

        # Break recursive cycles.
        if obj in hash_stacks.current:
            return _CYCLE_PLACEHOLDER

        hash_stacks.current.push(obj)

        try:
            # Hash the input
            b = b"%s:%s" % (tname, self._to_bytes(obj))

            # Hmmm... It's possible that the size calculation is wrong. When we
            # call to_bytes inside _to_bytes things get double-counted.
            self.size += sys.getsizeof(b)

            if key[1] is not NoResult:
                self._hashes[key] = b

        finally:
            # In case an UnhashableTypeError (or other) error is thrown, clean up the
            # stack so we don't get false positives in future hashing calls
            hash_stacks.current.pop()

        return b

    def update(self, hasher, obj: Any) -> None:
        """Update the provided hasher with the hash of an object."""
        b = self.to_bytes(obj)
        hasher.update(b)

    def _to_bytes(self, obj: Any) -> bytes:
        """Hash objects to bytes, including code with dependencies.
        
        Python's built in `hash` does not produce consistent results across
        runs.
        """
        if isinstance(obj, unittest.mock.Mock):
            # Mock objects can appear to be infinitely
            # deep, so we don't try to hash them at all.
            return self.to_bytes(id(obj))

        elif isinstance(obj, bytes) or isinstance(obj, bytearray):
            return obj

        elif isinstance(obj, str):
            return obj.encode()

        elif isinstance(obj, float):
            return self.to_bytes(hash(obj))

        elif isinstance(obj, int):
            return int_to_bytes(obj)

        elif isinstance(obj, (list, tuple)):
            h = hashlib.new("md5")
            for item in obj:
                self.update(h, item)
            return h.digest()

        elif isinstance(obj, dict):
            h = hashlib.new("md5")
            for item in obj.items():
                self.update(h, item)
            return h.digest()

        elif obj is None:
            return b"0"

        elif obj is True:
            return b"1"

        elif obj is False:
            return b"0"

        elif dataclasses.is_dataclass(obj):
            return self.to_bytes(dataclasses.asdict(obj))

        elif isinstance(obj, Enum):
            return str(obj).encode()

        elif inspect.isbuiltin(obj):
            return bytes(obj.__name__.encode())

        elif is_type(obj, "builtins.mappingproxy") or is_type(
            obj, "builtins.dict_items"
        ):
            return self.to_bytes(dict(obj))

        elif is_type(obj, "builtins.getset_descriptor"):
            return bytes(obj.__qualname__.encode())

        elif hasattr(obj, "name") and (
            isinstance(obj, io.IOBase)
            or isinstance(obj, tempfile._TemporaryFileWrapper)
        ):
            # Hash files as name + last modification date + offset.
            # NB: we're using hasattr("name") to differentiate between
            # on-disk and in-memory StringIO/BytesIO file representations.
            # That means that this condition must come before the next
            # condition, which just checks for StringIO/BytesIO.
            h = hashlib.new("md5")
            obj_name = getattr(obj, "name", "wonthappen")
            self.update(h, obj_name)
            self.update(h, os.path.getmtime(obj_name))
            self.update(h, obj.tell())
            return h.digest()

        elif isinstance(obj, Pattern):
            return self.to_bytes([obj.pattern, obj.flags])

        elif isinstance(obj, io.StringIO) or isinstance(obj, io.BytesIO):
            # Hash in-memory StringIO/BytesIO by their full contents
            # and seek position.
            h = hashlib.new("md5")
            self.update(h, obj.tell())
            self.update(h, obj.getvalue())
            return h.digest()

        elif inspect.ismodule(obj):
            return self.to_bytes(obj.__name__)

        elif inspect.isclass(obj):
            return self.to_bytes(obj.__name__)

        elif isinstance(obj, functools.partial):
            # The return value of functools.partial is not a plain function:
            # it's a callable object that remembers the original function plus
            # the values you pickled into it. So here we need to special-case it.
            h = hashlib.new("md5")
            self.update(h, obj.args)
            self.update(h, obj.func)
            self.update(h, obj.keywords)
            return h.digest()

        elif hasattr(obj, "__cache__"):
            # Non hashable objects can define a __cache__ method that returns
            # a stable key across runs
            return getattr(obj, "__cache__")()

        else:
            # As a last resort, hash the output of the object's __reduce__ method
            h = hashlib.new("md5")
            try:
                reduce_data = obj.__reduce__()
            except Exception as e:
                raise UnhashableTypeError() from e

            for item in reduce_data:
                self.update(h, item)
            return h.digest()