import logging
from datetime import datetime
from typing import List, Tuple

import anyio
from fastapi import APIRouter, Depends, Query, WebSocket
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse

from hyprxa.auth import BaseUser
from hyprxa.base import BaseSubscriber, iter_subscribers
from hyprxa.dependencies.auth import can_read, can_write
from hyprxa.dependencies.timeseries import (
    find_one_unitop,
    get_subscribers,
    get_subscriptions,
    get_timeseries_collection,
    get_unitop,
    get_unitop_collection,
    get_unitops,
    validate_sources
)
from hyprxa.dependencies.util import get_file_writer, parse_timestamp
from hyprxa.timeseries import (
    AnySourceSubscriptionRequest,
    AvailableSources,
    SubscriptionMessage,
    UnitOp,
    UnitOpDocument,
    UnitOpQueryResult,
    get_timeseries,
    SOURCES
)
from hyprxa.util.filestream import FileWriter, chunked_transfer
from hyprxa.util.formatting import format_event_document
from hyprxa.util.status import Status, StatusOptions
from hyprxa.util.sse import sse_handler
from hyprxa.util.websockets import ws_handler



_LOGGER = logging.getLogger("hyprxa.api.timeseries")

router = APIRouter(prefix="/timeseries", tags=["Timeseries"])


@router.get("/sources", response_model=AvailableSources, dependencies=[Depends(can_read)])
async def sources() -> AvailableSources:
    """Retrieve a list of the available data sources."""
    return {"sources": [source.source for source in SOURCES]}


@router.get("/unitops/{unitop}", response_model=UnitOpDocument, dependencies=[Depends(can_read)])
async def unitop(
    document: UnitOpDocument = Depends(get_unitop)
) -> UnitOpDocument:
    """Retrieve a unitop record."""
    return document


@router.get("/topics/search", response_model=UnitOpQueryResult, dependencies=[Depends(can_read)])
async def unitops(
    unitops: UnitOpQueryResult = Depends(get_unitops)
) -> UnitOpQueryResult:
    """Retrieve a collection of unitop records."""
    return unitops


@router.post("/unitops/save", response_model=Status)
async def save(
    unitop: UnitOp = Depends(validate_sources),
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection),
    user: BaseUser = Depends(can_write)
) -> Status:
    """Save a topic to the database."""
    await anyio.to_thread.run_sync(find_one_unitop.invalidate, unitop.name, collection)
    result = await collection.update_one(
        filter={"name": unitop.name},
        update={
            "$set": {
                "data_mapping": unitop.data_mapping,
                "meta": unitop.meta,
                "modified_by": user.identity,
                "modified_at": datetime.utcnow()
            }
        },
        upsert=True
    )
    if result.modified_count > 0 or result.matched_count > 0 or result.upserted_id:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.get("/stream/{unitop}", response_model=SubscriptionMessage, dependencies=[Depends(can_read)])
async def stream(
    subscribers: List[BaseSubscriber] = Depends(get_subscribers)
) -> SubscriptionMessage:
    """Subscribe to a unitop and stream timeseries data. This is an event sourcing
    (SSE) endpoint.
    """
    send = iter_subscribers(*subscribers)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{unitop}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(can_read),
    subscribers: List[BaseSubscriber] = Depends(get_subscribers)
) -> SubscriptionMessage:
    """Subscribe to a unitop and stream timeseries data over the websocket protocol."""
    try:
        await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        for subscriber in subscribers: subscriber.stop()
        return
    send = iter_subscribers(*subscriber)
    await ws_handler(websocket, _LOGGER, None, send)


@router.get("/{unitop}/recorded", response_class=StreamingResponse, dependencies=[Depends(can_read)])
async def timeseries(
    unitop: UnitOpDocument = Depends(get_unitop),
    subscriptions: AnySourceSubscriptionRequest = Depends(get_subscriptions),
    start_time: datetime = Depends(parse_timestamp(
        query=Query(default=None, alias="start_time"),
        default_timedelta=3600
    )),
    end_time: datetime = Depends(parse_timestamp(query=Query(default=None, alias="end_time"))),
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
    scan_rate: int = 5,
    file_writer: FileWriter = Depends(get_file_writer),
) -> StreamingResponse:
    """Get a batch of recorded events."""

    send = get_timeseries(
        collection=collection,
        subscriptions=subscriptions,
        start_time=start_time,
        end_time=end_time,
        scan_rate=scan_rate
    )

    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )

    groups = subscriptions.group()
    subscriptions: List[Tuple[int, str]] = []
    for source in groups:
        for subscription in groups[source]:
            subscriptions.append((hash(subscription), source))
    hashes = sorted(subscriptions)

    headers = []
    for hash_, source in hashes:
        for name, subscription in unitop.data_mapping.items():
            if hash(subscription) == hash_ and subscription.source == source:
                headers.append(name)

    assert len(headers) == len(hashes)

    chunk_size = min(int(100_0000/len(headers)), 5000)
    writer(["timestamp", *headers])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-events.{suffix}"
    )

    return StreamingResponse(
        chunked_transfer(
            send=send,
            buffer=buffer,
            writer=writer,
            formatter=format_event_document,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )