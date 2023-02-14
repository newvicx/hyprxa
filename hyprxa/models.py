import hashlib

import orjson
from pydantic import BaseModel

from hyprxa.util.json import json_dumps, json_loads



class BaseSubscription(BaseModel):
    """A hashable base model.
    
    Models must be json encode/decode(able). Hashes use the JSON string
    representation of the object and are consistent across runtimes.

    Hashing: The `dict()` representation of the model is converted to a JSON
    byte string which is then sorted. The hashing algorithm used is SHAKE 128
    with a 16 byte length. Finally, the hex digest is converted to a base 10
    integer.

    Note: Implementations must not override the comparison operators.
    These operators are based on the hash of the model which is critical when
    sorting sequences of mixed implementation types.
    """
    class Config:
        frozen=True
        json_dumps=json_dumps
        json_loads=json_loads

    def __hash__(self) -> int:
        try:
            o = bytes(sorted(orjson.dumps(self.dict())))
        except Exception as e:
            raise TypeError(f"unhashable type: {e.__str__()}")
        return int(hashlib.shake_128(o).hexdigest(16), 16)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            return False
        try:
            return hash(self) == hash(__o)
        except TypeError:
            return False
    
    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) > hash(__o)
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) < hash(__o)
        except TypeError:
            return False