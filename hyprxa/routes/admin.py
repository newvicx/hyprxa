import itertools
import logging

import anyio
from fastapi import APIRouter, Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
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
    `MongoLogHandler` and the database must be replica set.
    """
    try:
        await db.admin.command("replSetGetStatus")
    except OperationFailure:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Application database is not a replica set. Cannot stream logs."
        )
    
    handlers = [logging.getLogger(name).handlers for name in logging.root.manager.loggerDict]
    handlers.append(logging.root.handlers)

    for handler in itertools.chain.from_iterable(handlers):
        if isinstance(handler, MongoLogHandler):
            worker = handler.get_worker()
            break
    else:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Logging to database is not configured."
        )
    
    await anyio.to_thread.run_sync(worker.wait, 2)
    
    if worker.is_stopped:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Log worker is stopped."
        )
    elif not worker.is_running:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Log worker is not running.",
            headers={"Retry-After": 2}
        )
    collection = db[worker._database_name][worker._collection_name]
    send = watch_collection(collection)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)
