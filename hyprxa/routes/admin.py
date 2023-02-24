import io
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import Json
from sse_starlette import EventSourceResponse

from hyprxa.dependencies.auth import is_admin
from hyprxa.dependencies.info import Info, get_info
from hyprxa.dependencies.logs import get_logs_collection
from hyprxa.util.filestream import chunked_transfer, jsonlines_writer
from hyprxa.util.mongo import watch_collection
from hyprxa.util.sse import sse_handler



_LOGGER = logging.getLogger("hyprxa.api.admin")

router = APIRouter(prefix="/admin", tags=["Admin"], dependencies=[Depends(is_admin)])


@router.get("/info", response_model=Info)
async def info(info: Info = Depends(get_info)) -> Info:
    """"Return diagnostic information about brokers."""
    return info


@router.get("/logs/stream", response_class=EventSourceResponse)
async def logs(
    collection: AsyncIOMotorCollection = Depends(get_logs_collection(exclusive=True, check_is_replica_set=True))
) -> EventSourceResponse:
    """Stream logs from the API. The logging configuration must be using the
    `MongoLogHandler` and the database must be replica set.
    """
    send = watch_collection(collection)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.get("/logs/recorded", response_class=StreamingResponse)
async def recorded(
    q: Json = Query(default=None),
    collection: AsyncIOMotorCollection = Depends(get_logs_collection())
) -> StreamingResponse:
    """Get batch of logs. Supports arbitrary queries."""
    send = collection.find(q, projection={"_id": 0}).sort("timestamp", 1)

    buffer = io.StringIO()
    writer = jsonlines_writer(buffer)

    chunk_size = 1000

    filename = f"{int(datetime.utcnow().timestamp()*1_000_000)}.jsonl"

    return StreamingResponse(
        chunked_transfer(
            send=send,
            buffer=buffer,
            writer=writer,
            formatter=None,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type="application/x-jsonlines",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )