from contextvars import ContextVar

from hyprxa.auth import BaseUser



ip_address_context: ContextVar[str | None] = ContextVar("ip_address_context", default=None)
user_context: ContextVar[BaseUser | None] = ContextVar("user_context", default=None)