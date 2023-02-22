import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt

from hyprxa.auth.models import Token, TokenHandler
from hyprxa.auth.protocols import AuthenticationClient
from hyprxa.dependencies.auth import get_auth_client, get_token_handler



async def token(
    form: OAuth2PasswordRequestForm = Depends(),
    handler: TokenHandler = Depends(get_token_handler),
    client: AuthenticationClient = Depends(get_auth_client)
) -> Token:
    """Retrieve an access token for the API."""
    authenticated = await client.authenticate(form.username, form.password)
    if authenticated:
        claims = {"sub": form.username}
        access_token = handler.issue(claims=claims)
        return Token(access_token=access_token, token_type="bearer")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password"
    )


async def debug_token(form: OAuth2PasswordRequestForm = Depends()) -> Token:
    """Application is in debug mode. Token has no effect."""
    claims = {"sub": form.username}
    access_token = jwt.encode(claims=claims, key=secrets.token_hex(32), algorithm="HS256")
    return Token(access_token=access_token, token_type="bearer")