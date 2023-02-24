import itertools
import logging
from collections.abc import Awaitable
from typing import Callable

import anyio
from fastapi import Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import OperationFailure

from hyprxa.dependencies.db import get_exclusive_mongo_client, get_mongo_client
from hyprxa.logging.handlers import MongoLogHandler



def get_logs_collection(
    exclusive: bool = False,
    check_is_replica_set: bool = False
) -> Callable[[], Awaitable[AsyncIOMotorCollection]]:
    """Retrieve the logs collection from the logging config."""
    if exclusive or check_is_replica_set:
        dependency = get_exclusive_mongo_client
    else:
        dependency = get_mongo_client
        
    async def wrapper(db: AsyncIOMotorClient = Depends(dependency)) -> AsyncIOMotorCollection:
        if check_is_replica_set:
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
        return db[worker._database_name][worker._collection_name]

    return wrapper