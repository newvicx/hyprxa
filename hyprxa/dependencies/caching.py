import hashlib
from typing import Any, Callable, Type

from fastapi import HTTPException, status

from hyprxa.caching.hashing import update_hash
from hyprxa.caching.token import ReferenceToken, cache_tokenable
from hyprxa.util.caching import CacheType



def get_cached_reference(obj: Type[Any], raise_on_miss: bool = True) -> Callable[[str | None], Any | None]:
    """Dependency to recall a cached obj by a reference token."""
    def wrap(token: str | None = None) -> Any | None:
        if not token:
            return
        try:
            ret = cache_tokenable(token)
        except ValueError:
            if raise_on_miss:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Token was not found or it has expired."
                )
            return
        if not isinstance(ret, obj):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token does not reference a valid type."
            )
        return ret
    return wrap


def get_reference_token(obj: Type[Any]) -> Callable[[Any], ReferenceToken]:
    """Dependency to cache an object and produce a refrence token."""
    def wrap(o: obj) -> ReferenceToken:
        hasher = hashlib.new("md5")
        update_hash(val=o, hasher=hasher, cache_type=CacheType.MEMO)
        token = hasher.hexdigest()
        cache_tokenable(token, o)
        return ReferenceToken(token=token)
    return wrap