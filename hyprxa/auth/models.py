import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Set

from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, SecretStr
from starlette.authentication import BaseUser as StarletteBaseUser



_LOGGER = logging.getLogger("hyprxa.auth.models")



class BaseUser(BaseModel, StarletteBaseUser):
    """Base model for all commandcenter users."""
    username: str
    first_name: str | None
    last_name: str | None
    email: str | None
    upi: int | None
    company: str | None
    country: str | None
    scopes: Set[str] | None

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def identity(self) -> str:
        return self.username

    @property
    def display_name(self) -> str:
        return self.identity


class Token(BaseModel):
    """Access token model."""
    access_token: str
    token_type: str


class TokenHandler(BaseModel):
    """Model for issuing and validating JWT's."""
    key: SecretStr
    expire: timedelta = 1800
    algorithm: str = "HS256"

    def issue(self, claims: Dict[str, Any]) -> str:
        """Issue a JWT.
        
        Args:
            claims: The JWT token claims.
        
        Returns:
            token: The signed JWT.

        Raises:
            JWTError: Error encoding token.
        """
        to_encode = claims.copy()
        expire_at = datetime.utcnow() + self.expire
        to_encode.update({"exp": expire_at})
        
        return jwt.encode(
            to_encode,
            self.key.get_secret_value(),
            algorithm=self.algorithm
        )

    def validate(self, token: str) -> str | None:
        """Validate a JWT and return the username.
        
        Args:
            token: The token to validate.

        Returns:
            username: The username the token is associated to. If the token is
                invalid, returns `None`
        """
        try:
            payload = jwt.decode(
                token,
                self.key.get_secret_value(),
                algorithms=[self.algorithm]
            )
            return payload.get("sub")
        except ExpiredSignatureError:
            _LOGGER.debug("Token expired")
            return
        except JWTError:
            _LOGGER.debug("Received invalid token")
            return