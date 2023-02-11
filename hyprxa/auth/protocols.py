from typing import Protocol

from hyprxa.auth.models import BaseUser



class AuthenticationClient(Protocol):
    async def authenticate(self, username: str, password: str) -> bool:
        """Authenticate a username and password against an authority.
        
        Args:
            username: Username.
            password: Password.
        
        Returns:
            authenticated: `True` if valid user credentials, `False` otherwise.
        """
        ...
    
    async def get_user(self, username: str) -> BaseUser:
        """Retrieve user information from an authority.
        
        Args:
            username: Username.
        
        Returns:
            user: `BaseUser` with populated user information.

        Raises:
            UserNotFound: The user was not found.
        """
        ...