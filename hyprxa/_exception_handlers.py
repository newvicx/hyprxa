import logging

from fastapi import status
from fastapi.responses import JSONResponse
from pymongo.errors import (
    AutoReconnect,
    WaitQueueTimeoutError,
    PyMongoError
)
from starlette.requests import HTTPConnection

from hyprxa.base.exceptions import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching.exceptions import CacheError
from hyprxa._exceptions import NotConfiguredError
from hyprxa.timeseries.exceptions import (
    IntegrationSubscriptionError,
    SubscriptionLockError
)
from hyprxa.util.mongo import DatabaseUnavailable



_LOGGER = logging.getLogger("hyprxa.exceptions")


async def handle_NotConfiguredError(
    connection: HTTPConnection,
    exc: NotConfiguredError
) -> JSONResponse:
    """Exception handler for `NotConfiguredError`. Return 501 response."""
    _LOGGER.warning(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={"detail": str(exc)}
    )


async def handle_PyMongoError(
    connection: HTTPConnection,
    exc: PyMongoError
) -> JSONResponse:
    """Exception handler for `PyMongoError`. Return 500 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error in application database. Contact an administrator."}
    )


async def handle_DatabaseUnavailable(
    connection: HTTPConnection,
    exc: DatabaseUnavailable | AutoReconnect | WaitQueueTimeoutError
) -> JSONResponse:
    """Exception handler for `DatabaseUnavailable`, `AutoReconnect`,
    `WaitQueueTimeoutError`. Return 503 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Application database is unavailable."},
        headers={"Retry-After": 5}
    )


async def handle_ManagerClosed(
    connection: HTTPConnection,
    exc: ManagerClosed
) -> JSONResponse:
    """Exception handler for `ManagerClosed`. Return 500 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Manager is closed, the service must be restarted. Contact an administrator."}
    )


async def handle_retryable_SubscriptionError(
    connection: HTTPConnection,
    exc: SubscriptionLimitError | SubscriptionLockError | SubscriptionTimeout
) -> JSONResponse:
    """Exception handler for `SubscriptionLimitError`, `SubscriptionLockError`,
    and `SubscriptionTimeout`. Return 503 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Broker is unavailable."},
        headers={"Retry-After": 5}
    )


async def handle_IntegrationSubscriptionError(
    connection: HTTPConnection,
    exc: IntegrationSubscriptionError
) -> JSONResponse:
    """Exception handler for `IntegrationSubscriptionError`. Return 500 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)}
    )


async def handle_CacheError(
    connection: HTTPConnection,
    exc: CacheError
) -> JSONResponse:
    """Exception handler for `CacheError`. Return 500 response."""
    _LOGGER.error(f"Error in {connection.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error in caching server. Contact an adminstrator."}
    )