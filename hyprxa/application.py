import itertools
from collections.abc import Coroutine, Sequence
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union
)

from fastapi import Depends, FastAPI, Request, Response, routing
from fastapi.datastructures import Default
from fastapi.middleware import Middleware
from fastapi.utils import generate_unique_id
from pymongo.errors import PyMongoError
from starlette.authentication import AuthenticationBackend
from fastapi.middleware.asyncexitstack import AsyncExitStackMiddleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.errors import ServerErrorMiddleware
from starlette.middleware.exceptions import ExceptionMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from starlette.types import ASGIApp

import hyprxa.__version__
from hyprxa.auth.base import BaseAuthenticationBackend, on_error
from hyprxa.auth.debug import DebugAuthenticationMiddleware, enable_interactive_auth
from hyprxa.auth.models import BaseUser, Token
from hyprxa.auth.protocols import AuthenticationClient
from hyprxa.auth.route import debug_token, token
from hyprxa.exceptions import (
    CacheError,
    DatabaseUnavailable,
    IntegrationSubscriptionError,
    ManagerClosed,
    NotConfiguredError,
    SubscriptionLimitError,
    SubscriptionLockError,
    SubscriptionTimeout,
    handle_CacheError,
    handle_DatabaseUnavailable,
    handle_IntegrationSubscriptionError,
    handle_ManagerClosed,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError
)
from hyprxa.middleware import (
    CorrelationIDMiddleware,
    IPAddressMiddleware,
    UserMiddleware
)
from hyprxa.routes import (
    admin_router,
    events_router,
    timeseries_router,
    topics_router,
    unitops_router,
    users_router
)
from hyprxa.settings import HYPRXA_SETTINGS, LOGGING_SETTINGS, SENTRY_SETTINGS
from hyprxa.timeseries.base import BaseIntegration
from hyprxa.timeseries.sources import add_source



_ADMIN_USER = BaseUser(
    username="admin",
    first_name="admin",
    last_name="admin",
    email="admin@noreturn.com",
    upi=19961902,
    company="hyprxa",
    scopes=set(
        itertools.chain(
            HYPRXA_SETTINGS.read_scopes,
            HYPRXA_SETTINGS.write_scopes,
            HYPRXA_SETTINGS.admin_scopes
        )
    )
)


class Hyprxa(FastAPI):
    def __init__(
        self,
        *,
        debug: bool = False,
        interactive_auth: bool = False,
        token_path: str = "/token",
        auth_client: Optional[Callable[[], AuthenticationClient]] = None,
        auth_backend: Optional[Type[BaseAuthenticationBackend]] = None,
        routes: Optional[List[BaseRoute]] = None,
        title: str = hyprxa.__version__.__title__,
        description: str = hyprxa.__version__.__description__,
        version: str = hyprxa.__version__.__version__,
        openapi_url: Optional[str] = "/openapi.json",
        openapi_tags: Optional[List[Dict[str, Any]]] = None,
        servers: Optional[List[Dict[str, Union[str, Any]]]] = None,
        dependencies: Optional[Sequence[Depends]] = None,
        default_response_class: Type[Response] = Default(JSONResponse),
        docs_url: Optional[str] = "/docs",
        redoc_url: Optional[str] = "/redoc",
        swagger_ui_oauth2_redirect_url: Optional[str] = "/docs/oauth2-redirect",
        swagger_ui_init_oauth: Optional[Dict[str, Any]] = None,
        middleware: Optional[Sequence[Middleware]] = None,
        exception_handlers: Optional[
            Dict[
                Union[int, Type[Exception]],
                Callable[[Request, Any], Coroutine[Any, Any, Response]],
            ]
        ] = None,
        on_startup: Optional[Sequence[Callable[[], Any]]] = None,
        on_shutdown: Optional[Sequence[Callable[[], Any]]] = None,
        terms_of_service: Optional[str] = None,
        contact: Optional[Dict[str, Union[str, Any]]] = None,
        license_info: Optional[Dict[str, Union[str, Any]]] = None,
        openapi_prefix: str = "",
        root_path: str = "",
        root_path_in_servers: bool = True,
        responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
        callbacks: Optional[List[BaseRoute]] = None,
        deprecated: Optional[bool] = None,
        include_in_schema: bool = True,
        swagger_ui_parameters: Optional[Dict[str, Any]] = None,
        generate_unique_id_function: Callable[[routing.APIRoute], str] = Default(
            generate_unique_id
        ),
        **extra: Any
    ) -> None:
        if not debug and (auth_client is None or auth_backend is None):
            raise ValueError(
                "'auth_client' and 'auth_backend' cannot be `Type[None]` when 'debug' is `False`"
            )
        
        LOGGING_SETTINGS.configure_logging()
        SENTRY_SETTINGS.configure_sentry()

        dependencies = dependencies or []
        if interactive_auth:
            dependencies.append(Depends(enable_interactive_auth(token_path)))
        
        super().__init__(
            debug=debug,
            routes=routes,
            title=title,
            description=description,
            version=version,
            openapi_url=openapi_url,
            openapi_tags=openapi_tags,
            servers=servers,
            dependencies=dependencies,
            default_response_class=default_response_class,
            docs_url=docs_url,
            redoc_url=redoc_url,
            swagger_ui_oauth2_redirect_url=swagger_ui_oauth2_redirect_url,
            swagger_ui_init_oauth=swagger_ui_init_oauth,
            middleware=middleware,
            exception_handlers=exception_handlers,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            terms_of_service=terms_of_service,
            contact=contact,
            license_info=license_info,
            openapi_prefix=openapi_prefix,
            root_path=root_path,
            root_path_in_servers=root_path_in_servers,
            responses=responses,
            callbacks=callbacks,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            swagger_ui_parameters=swagger_ui_parameters,
            generate_unique_id_function=generate_unique_id_function,
            **extra
        )
        self.auth_client = auth_client
        self.auth_backend = auth_backend

        self.include_router(admin_router)
        self.include_router(events_router)
        self.include_router(timeseries_router)
        self.include_router(topics_router)
        self.include_router(unitops_router)
        self.include_router(users_router)

        self.add_exception_handler(CacheError, handle_CacheError)
        self.add_exception_handler(DatabaseUnavailable, handle_DatabaseUnavailable)
        self.add_exception_handler(IntegrationSubscriptionError, handle_IntegrationSubscriptionError)
        self.add_exception_handler(ManagerClosed, handle_ManagerClosed)
        self.add_exception_handler(NotConfiguredError, handle_NotConfiguredError)
        self.add_exception_handler(PyMongoError, handle_PyMongoError)
        self.add_exception_handler(SubscriptionLimitError, handle_retryable_SubscriptionError)
        self.add_exception_handler(SubscriptionLockError, handle_retryable_SubscriptionError)
        self.add_exception_handler(SubscriptionTimeout, handle_retryable_SubscriptionError)

        if self.debug:
            self.add_api_route(token_path, debug_token, response_model=Token, tags=["Token"], methods=["POST"])
        else:
            self.add_api_route(token_path, token, response_model=Token, tags=["Token"], methods=["POST"])

        self._authentication_middleware: Middleware = None

    @property
    def authentication_middleware(self) -> Middleware | None:
        return self._authentication_middleware

    def add_source(
        self,
        source: str,
        integration: Callable[..., BaseIntegration],
        scopes: Sequence[str] | None = None,
        any_: bool = False,
        raise_on_no_scopes: bool = False,
        *client_args: Any,
        **client_kwargs: Any
    ) -> None:
        """Add a data source integration to the API."""
        if scopes is not None:
            for scope in scopes:
                _ADMIN_USER.scopes.add(scope)

        add_source(
            source=source,
            integration=integration,
            scopes=scopes,
            any_=any_,
            raise_on_no_scopes=raise_on_no_scopes,
            *client_args,
            **client_kwargs
        )

    def build_middleware_stack(self) -> ASGIApp:
        # Duplicate/override from FastAPI to add Hyprxa default middleware
        # stack before user middleware
        debug = self.debug
        error_handler = None
        exception_handlers = {}

        for key, value in self.exception_handlers.items():
            if key in (500, Exception):
                error_handler = value
            else:
                exception_handlers[key] = value

        if debug:
            authentication = DebugAuthenticationMiddleware
            authentication.set_user(_ADMIN_USER)
            backend = AuthenticationBackend()
        else:
            authentication = AuthenticationMiddleware
            client = self.auth_client()
            handler = HYPRXA_SETTINGS.get_token_handler()
            backend = self.auth_backend(
                handler=handler,
                client=client
            )

        authentication_middleware = Middleware(
            authentication,
            backend=backend,
            on_error=on_error
        )

        middleware = (
            [Middleware(ServerErrorMiddleware, handler=error_handler, debug=debug)]
            + [
                Middleware(
                    CORSMiddleware,
                    allow_origins=HYPRXA_SETTINGS.allow_origins,
                    allow_credentials=True,
                    allow_methods=["*"],
                    allow_headers=["*"]
                ),
                authentication_middleware,
                Middleware(CorrelationIDMiddleware),
                Middleware(IPAddressMiddleware),
                Middleware(UserMiddleware)
            ]
            + self.user_middleware
            + [
                Middleware(
                    ExceptionMiddleware, handlers=exception_handlers, debug=debug
                ),
                Middleware(AsyncExitStackMiddleware),
            ]
        )

        self._authentication_middleware = authentication_middleware

        app = self.router
        for cls, options in reversed(middleware):
            app = cls(app=app, **options)
        return app

    def add_admin_scopes(scopes: Sequence[str] | None) -> None:
        """Add scopes to the ADMIN user profile."""
        if scopes:
            for scope in scopes:
                _ADMIN_USER.scopes.add(scope)