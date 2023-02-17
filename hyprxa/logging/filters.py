import logging
import socket

from hyprxa._context import ip_address_context, user_context



HOST = socket.getaddrinfo(socket.gethostname(), 0, flags=socket.AI_CANONNAME)[0][3]


class HostFilter(logging.Filter):
    """Logging filter that adds the host to each log record."""
    def filter(self, record: logging.LogRecord) -> bool:
        record.host = HOST
        return True


class IPAddressFilter(logging.Filter):
    """Logging filter that adds the client IP Address to each log record.
    
    The `IPAddressMiddleware` must be installed for this filter.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        ip_address = ip_address_context.get()
        if ip_address is not None:
            record.ip_address = ip_address
        return True


class UserFilter(logging.Filter):
    """Logging filter that adds the client username to each log record.
    
    The `UserMiddlerware` must be installed for this filter.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        user = user_context.get()
        if user is not None:
            record.user = user.identity
        return True