from typing import Any

from pydantic import BaseModel

from hyprxa.caching.memo import memo



@memo(ttl=3600)
def cache_tokenable(
    token: str,
    _obj: Any | None = None
) -> Any:
    """Utilizes the mechanics of the memo cache to pass both a token and tokenable
    object and then recall the object by just passing the token.

    Note: The object must be pickleable.

    Raises:
        ValueError: Invalid token.
    """
    if not _obj:
        raise ValueError("Invalid token.")
    return _obj


class ReferenceToken(BaseModel):
    token: str