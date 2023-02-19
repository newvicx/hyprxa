import itertools
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from sse_starlette import EventSourceResponse

from hyprxa.dependencies.auth import is_admin
from hyprxa.dependencies.db import get_exclusive_mongo_client
from hyprxa.dependencies.info import Info, get_info
from hyprxa.logging.handlers import MongoLogHandler
from hyprxa.util.mongo import watch_collection
from hyprxa.util.sse import sse_handler



_LOGGER = logging.getLogger("hyprxa.api.admin")

router = APIRouter(prefix="/admin", tags=["Admin"], dependencies=[Depends(is_admin)])


@router.get("/info", response_model=Info)
async def info(info: Info = Depends(get_info)) -> Info:
    """"Return diagnostic information about brokers."""
    return info


@router.get("/logs", response_class=EventSourceResponse)
async def logs(
    db: AsyncIOMotorClient = Depends(get_exclusive_mongo_client)
) -> EventSourceResponse:
    """Stream logs from the API. The logging configuration must be using the
    `MongoLogHandler`.
    """
    handlers = itertools.chain.from_iterable(
        [logging.getLogger(name).handlers for name in logging.root.manager.loggerDict]
    )
    for handler in handlers:
        if isinstance(handler, MongoLogHandler):
            worker = handler.get_worker()
            break
    else:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Logging to database is not configured."
        )
    if not worker.is_running or worker.is_stopped:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Log worker is not running or is stopped."
        )
    collection = db[worker._database_name][worker._collection_name]
    send = watch_collection(collection)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)
