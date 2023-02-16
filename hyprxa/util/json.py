import json
from typing import Any

import orjson



def json_loads(v: str | bytes) -> Any:
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    match v:
        case str():
            return json.loads(v)
        case bytes():
            return orjson.loads(v)
        case _:
            raise TypeError(f"Expected str | bytes, got {type(v)}")


def json_dumps(obj: Any, as_bytes: bool = False, **dumps_kwargs) -> str | bytes:
    """JSON encoder which uses orjson for serializing data."""
    if as_bytes:
        try:
            return orjson.dumps(obj, **dumps_kwargs)
        except TypeError: # Got a keyword that orjson doesnt support such as 'indent'
            return json.dumps(obj, **dumps_kwargs).encode()
    try:
        return orjson.dumps(obj, **dumps_kwargs).decode()
    except TypeError:
        return json.dumps(obj, **dumps_kwargs)