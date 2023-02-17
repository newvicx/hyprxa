from fastapi import Depends, Request
from starlette.middleware.authentication import AuthenticationMiddleware

from hyprxa.auth.base import BaseAuthenticationBackend
from hyprxa.auth.models import BaseUser, TokenHandler
from hyprxa.auth.protocols import AuthenticationClient
from hyprxa.auth.scopes import requires
from hyprxa.exceptions import NotConfiguredError
from hyprxa.settings import HYPRXA_SETTINGS



async def can_read(
    user: BaseUser = Depends(
        requires(
            scopes=HYPRXA_SETTINGS.read_scopes,
            any_=HYPRXA_SETTINGS.read_any,
            raise_on_no_scopes=HYPRXA_SETTINGS.read_raise_on_no_scopes
        )
    )
) -> BaseUser:
    """Verifies user has read privilages."""
    return user


async def can_write(
    user: BaseUser = Depends(
        requires(
            scopes=HYPRXA_SETTINGS.write_scopes,
            any_=HYPRXA_SETTINGS.write_any,
            raise_on_no_scopes=HYPRXA_SETTINGS.write_raise_on_no_scopes
        )
    )
) -> BaseUser:
    """Verifies user has write privilages."""
    return user


async def is_admin(
    user: BaseUser = Depends(
        requires(
            scopes=HYPRXA_SETTINGS.admin_scopes,
            any_=HYPRXA_SETTINGS.admin_any,
            raise_on_no_scopes=HYPRXA_SETTINGS.admin_raise_on_no_scopes
        )
    )
) -> BaseUser:
    """Verifies user has admin privilages."""
    return user


async def get_auth_backend(request: Request) -> BaseAuthenticationBackend:
    """Get the authentication backed for the application."""
    for middleware in request.app.user_middleware:
        if issubclass(middleware.cls, AuthenticationMiddleware):
            backend = middleware.options.get("backend")
            if backend is not None:
                return backend
    else:
        raise NotConfiguredError("Authentication middleware not installed.")


async def get_auth_client(
    backend: BaseAuthenticationBackend = Depends(get_auth_backend)
) -> AuthenticationClient:
    """Get the authentication client for the application."""
    return backend.client


async def get_token_handler(
    backend: BaseAuthenticationBackend = Depends(get_auth_backend)
) -> TokenHandler:
    """Get the token handler for the application."""
    return backend.client