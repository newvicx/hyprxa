from contextvars import ContextVar

from hyprxa.auth import BaseUser



ip_address_context: ContextVar[str | None] = ContextVar("ip_address_context", default=None)
user_context: ContextVar[BaseUser | None] = ContextVar("user_context", default=None)


def get_username() -> str | None:
    """Get username from context."""
    user = user_context.get()
    if user is not None:
        return user.identity