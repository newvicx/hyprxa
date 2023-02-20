import itertools

from fastapi import FastAPI
from starlette.authentication import AuthenticationBackend

from pymongo.errors import PyMongoError

from hyprxa.auth import Token, token
from hyprxa.base import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching import CacheError
from hyprxa.exceptions import NotConfiguredError
from hyprxa.timeseries import (
    ClientSubscriptionError,
    SubscriptionLockError
)
from hyprxa.util.mongo import DatabaseUnavailable

from hyprxa.auth import BaseUser
from hyprxa.auth.debug import DebugAuthenticationMiddleware
from hyprxa.exception_handlers import (
    handle_ManagerClosed,
    handle_CacheError,
    handle_ClientSubscriptionError,
    handle_DatabaseUnavailable,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError,
)
from hyprxa.middleware import CorrelationIDMiddleware, IPAddressMiddleware, UserMiddleware
from hyprxa.routes import (
    admin_router,
    events_router,
    timeseries_router,
    topics_router,
    unitops_router,
    users_router
)
from hyprxa.settings import HYPRXA_SETTINGS, LOGGING_SETTINGS


ADMIN_USER = BaseUser(
    username="admin",
    first_name="Christopher",
    last_name="Newville",
    email="chrisnewville1396@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            HYPRXA_SETTINGS.admin_scopes,
            HYPRXA_SETTINGS.write_scopes,
            HYPRXA_SETTINGS.read_scopes
        )
    )
)

LOGGING_SETTINGS.configure_logging()

app = FastAPI(
    debug=True,
    title="hyprxa-demo",
    description="Hyprxa timeseries and event hub demo."
)

DebugAuthenticationMiddleware.set_user(ADMIN_USER)

app.add_middleware(UserMiddleware)
app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(IPAddressMiddleware)
app.add_middleware(DebugAuthenticationMiddleware, backend=AuthenticationBackend())

app.include_router(admin_router)
app.include_router(events_router)
app.include_router(timeseries_router)
app.include_router(topics_router)
app.include_router(unitops_router)
app.include_router(users_router)

app.add_exception_handler(NotConfiguredError, handle_NotConfiguredError)
app.add_exception_handler(PyMongoError, handle_PyMongoError)
app.add_exception_handler(DatabaseUnavailable, handle_DatabaseUnavailable)
app.add_exception_handler(ManagerClosed, handle_ManagerClosed)
app.add_exception_handler(ClientSubscriptionError, handle_ClientSubscriptionError)
app.add_exception_handler(SubscriptionTimeout, handle_retryable_SubscriptionError)
app.add_exception_handler(SubscriptionLimitError, handle_retryable_SubscriptionError)
app.add_exception_handler(SubscriptionLockError, handle_retryable_SubscriptionError)
app.add_exception_handler(CacheError, handle_CacheError)


app.add_api_route("/token", token, response_model=Token, tags=["Login"])