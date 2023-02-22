from typing import Tuple

from starlette.authentication import (
    AuthCredentials,
    AuthenticationBackend,
    AuthenticationError
)
from starlette.requests import HTTPConnection
from starlette.responses import JSONResponse, Response

from hyprxa.auth.models import BaseUser, TokenHandler
from hyprxa.auth.protocols import AuthenticationClient



class BaseAuthenticationBackend(AuthenticationBackend):
    """Base class for all authentication backends.
    
    Args:
        handler: A `TokenHandler` for issuing and validating tokens.
        client: An `AuthenticationClient` that queires an authentication/authorization
            database.
    """
    def __init__(self, handler: TokenHandler, client: AuthenticationClient) -> None:
        self.handler = handler
        self.client = client

    async def authenticate(self, conn: HTTPConnection) -> Tuple[AuthCredentials, BaseUser] | None:
        """Validate a token from the connection and return a user along with
        their scopes.
        
        Exceptions originating from the client must be caught and re-raised
        into an `AuthenticationError`.

        If a user is unauthenticated, this method may return `None` or some
        other unauthenticated user class.
        """
        raise NotImplementedError()
    

def on_error(conn: HTTPConnection, err: AuthenticationError) -> Response:
    """An error handler function for the `AuthenticationMiddleware`."""
    return JSONResponse(
        {"detail": f"The request cannot be completed. {str(err)}"},
        status_code=500
    )