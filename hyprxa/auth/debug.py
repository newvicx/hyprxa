from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.types import Receive, Scope, Send

from hyprxa.auth.models import BaseUser



def enable_interactive_auth(path: str) -> None:
    """Dependency that enables authorization in the interactive docs."""
    scheme = OAuth2PasswordBearer(path, auto_error=False)
    async def async_wrapper(_: str | None = Depends(scheme)) -> None:
        pass
    return async_wrapper


class DebugAuthenticationMiddleware(AuthenticationMiddleware):
    """Authentication middleware for debug mode ONLY. This always return an
    admin user regardless of the backend.

    Examples:
    Use this middleware just like the `AuthenticationMiddleware` from starlette...
    >>> middleware = Middleware(
    ...     DebugAuthenticationMiddelware,
    ...     backend=backend,
    ...     on_error=on_error
    ... )

    Dont forget to set the admin user though...
    >>> DebugAuthenticationMiddleware.set_user(...)
    """
    _admin_user: BaseUser = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            await self.app(scope, receive, send)
            return
        scope["auth"], scope["user"] = self._admin_user.scopes, self._admin_user
        await self.app(scope, receive, send)

    @classmethod
    def set_user(cls, user: BaseUser) -> None:
        cls._admin_user = user