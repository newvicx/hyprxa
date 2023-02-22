from hyprxa.auth.exceptions import AuthError, UserNotFound
from hyprxa.base.exceptions import (
    DroppedSubscriber,
    ManagerClosed,
    ManagerError,
    SubscriptionError,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching.exceptions import (
    CacheError,
    CacheKeyNotFoundError,
    UnhashableParamError,
    UnserializableReturnValueError
)
from hyprxa.events.exceptions import EventManagerClosed
from hyprxa.timeseries.exceptions import (
    IntegrationClosed,
    IntegrationSubscriptionError,
    SubscriptionLockError,
    TimeseriesError,
    TimeseriesManagerClosed
)
from hyprxa.util.mongo import DatabaseUnavailable
from hyprxa._exception_handlers import (
    handle_CacheError,
    handle_DatabaseUnavailable,
    handle_IntegrationSubscriptionError,
    handle_ManagerClosed,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError
)
from hyprxa._exceptions import HyprxaError, NotConfiguredError



__all__ = [
    "AuthError",
    "UserNotFound",
    "DroppedSubscriber",
    "ManagerClosed",
    "ManagerError",
    "SubscriptionError",
    "SubscriptionLimitError",
    "SubscriptionTimeout",
    "CacheError",
    "CacheKeyNotFoundError",
    "UnhashableParamError",
    "UnserializableReturnValueError",
    "EventManagerClosed",
    "IntegrationClosed",
    "IntegrationSubscriptionError",
    "SubscriptionLockError",
    "TimeseriesError",
    "TimeseriesManagerClosed",
    "DatabaseUnavailable",
    "handle_CacheError",
    "handle_DatabaseUnavailable",
    "handle_IntegrationSubscriptionError",
    "handle_ManagerClosed",
    "handle_NotConfiguredError",
    "handle_PyMongoError",
    "handle_retryable_SubscriptionError",
    "HyprxaError",
    "NotConfiguredError",
]