import logging

from fastapi import Request, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from hyprxa.base import (
    BrokerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.exceptions import NotConfiguredError
from hyprxa.timeseries import (
    ClientSubscriptionError,
    SubscriptionLockError
)
from hyprxa.util.mongo import ServerUnavailable



_LOGGER = logging.getLogger("hyprxa.exceptions")


async def handle_NotConfiguredError(
    request: Request,
    exc: NotConfiguredError
) -> JSONResponse:
    _LOGGER.warning(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={"detail": str(exc)}
    )


async def handle_PyMongoError(
    request: Request,
    exc: PyMongoError
) -> JSONResponse:
    _LOGGER.error(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error in application database."}
    )


async def handle_MongoUnavailable(
    request: Request,
    exc: ServerUnavailable
) -> JSONResponse:
    _LOGGER.error(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Application database is unavailable."},
        headers={"Retry-After": 5}
    )


async def handle_BrokerClosed(
    request: Request,
    exc: BrokerClosed
) -> JSONResponse:
    _LOGGER.error(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Broker is closed, the service must be restarted. Contact an administrator."}
    )


async def handle_retryable_SubscriptionError(
    request: Request,
    exc: SubscriptionLimitError | SubscriptionLockError | SubscriptionTimeout
) -> JSONResponse:
    _LOGGER.error(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Broker is unavailable."},
        headers={"Retry-After": 5}
    )


async def handle_ClientSubscriptionError(
    request: Request,
    exc: ClientSubscriptionError
) -> JSONResponse:
    _LOGGER.error(f"Error in {request.path_params}", exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)}
    )