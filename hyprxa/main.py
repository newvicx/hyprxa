import itertools

from fastapi import FastAPI
from starlette.authentication import AuthenticationBackend

from pymongo.errors import PyMongoError

from hyprxa.base import (
    BrokerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout,
    SubscriptionError
)
from hyprxa.exceptions import NotConfiguredError
from hyprxa.timeseries import (
    ClientSubscriptionError,
    SubscriptionLockError
)
from hyprxa.util.mongo import ServerUnavailable

from hyprxa.auth import BaseUser
from hyprxa.auth.debug import DebugAuthenticationMiddleware
from hyprxa.exception_handlers import (
    handle_BrokerClosed,
    handle_ClientSubscriptionError,
    handle_MongoUnavailable,
    handle_NotConfigureError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError
)
from hyprxa.middleware import CorrelationIDMiddleware, IPAddressMiddleware, UserMiddleware
from hyprxa.routes.events import router as events
from hyprxa.routes.timeseries import router as timeseries
from hyprxa.routes.users import router as users
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
    title="hyprxa",
    description="Data integration and event hub."
)

DebugAuthenticationMiddleware.set_user(ADMIN_USER)

app.add_middleware(UserMiddleware)
app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(IPAddressMiddleware)
app.add_middleware(DebugAuthenticationMiddleware, backend=AuthenticationBackend())

app.include_router(events)
app.include_router(timeseries)
app.include_router(users)

app.add_exception_handler(NotConfiguredError, handle_NotConfigureError)
app.add_exception_handler(PyMongoError, handle_PyMongoError)
app.add_exception_handler(ServerUnavailable, handle_MongoUnavailable)
app.add_exception_handler(BrokerClosed, handle_BrokerClosed)
app.add_exception_handler(ClientSubscriptionError, handle_ClientSubscriptionError)
app.add_exception_handler(SubscriptionError, handle_retryable_SubscriptionError)
