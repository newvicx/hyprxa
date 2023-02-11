import logging

from asgi_correlation_id.middleware import CorrelationIdMiddleware, FAILED_VALIDATION_MESSAGE
from asgi_correlation_id.context import correlation_id
from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hyprxa.context import ip_address_context, user_context



_LOGGER = logging.getLogger("hyprxa.middleware")


class IPAddressMiddleware:
    """Middleware that sets the ip address context variable for logging.
    
    If working behind a proxy, be sure to set the '--proxy-headers' on uvicorn.
    """
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> Response:
        """Assign the user value from `scope['client'][0]` to the context variable."""
        if scope['type'] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        ip_address = None
        try:
            ip_address = scope.get("client", [])[0]
        except IndexError:
            pass
        if not ip_address:
            await self.app(scope, receive, send)
            return
        token = ip_address_context.set(ip_address)
        try:
            await self.app(scope, receive, send)
            return
        finally:
            ip_address_context.reset(token)


class UserMiddleware:
    """Middleware that sets the user context variable for logging.
    
    The `AuthenticationMiddlware` must be installed to set the context.
    """
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> Response:
        """Assign the user value from `scope['user']` to the context variable."""
        if scope['type'] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        user = scope.get("user")
        try:
            user.identity
        except: # AttributeError or NotImplementError
            user = None
        if not user:
            await self.app(scope, receive, send)
            return
        token = user_context.set(user)
        try:
            await self.app(scope, receive, send)
        finally:
            user_context.reset(token)


class CorrelationIDMiddlewareMod(CorrelationIdMiddleware):
    """Near carbon copy of the base CorrelationIDMiddleware but allows websocket scopes."""
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        # Try to load request ID from the request headers
        header_value = Headers(scope=scope).get(self.header_name.lower())

        if not header_value:
            # Generate request ID if none was found
            id_value = self.generator()
        elif self.validator and not self.validator(header_value):
            # Also generate a request ID if one was found, but it was deemed invalid
            id_value = self.generator()
            _LOGGER.warning(FAILED_VALIDATION_MESSAGE, header_value)
        else:
            # Otherwise, use the found request ID
            id_value = header_value

        # Clean/change the ID if needed
        if self.transformer:
            id_value = self.transformer(id_value)

        correlation_id.set(id_value)
        self.sentry_extension(id_value)

        async def handle_outgoing_request(message: Message) -> None:
            if message['type'] == 'http.response.start' and correlation_id.get():
                headers = MutableHeaders(scope=message)
                headers.append(self.header_name, correlation_id.get())
                headers.append('Access-Control-Expose-Headers', self.header_name)

            await send(message)

        if scope['type'] != "http":
            await self.app(scope, receive, send)
            return
        await self.app(scope, receive, handle_outgoing_request)