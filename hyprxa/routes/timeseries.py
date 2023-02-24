import logging
from datetime import datetime
from typing import List, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse
from starlette.websockets import WebSocket

from hyprxa.auth import BaseUser
from hyprxa.base import BaseSubscriber, iter_subscribers
from hyprxa.dependencies.auth import can_read, is_admin
from hyprxa.dependencies.timeseries import (
    get_subscribers,
    get_subscriptions,
    get_timeseries_collection,
    get_timeseries_manager
)
from hyprxa.dependencies.unitops import map_subscriptions
from hyprxa.dependencies.util import get_file_writer, parse_timestamp
from hyprxa.timeseries.manager import TimeseriesManager
from hyprxa.timeseries.models import AnySourceSubscriptionRequest, SubscriptionMessage
from hyprxa.timeseries.sources import AvailableSources, _SOURCES
from hyprxa.timeseries.stream import get_subscription_data, get_timeseries
from hyprxa.unitops.models import UnitOpDocument
from hyprxa.util.filestream import FileWriter, chunked_transfer
from hyprxa.util.formatting import format_timeseries_rows
from hyprxa.util.sse import sse_handler
from hyprxa.util.status import Status, StatusOptions
from hyprxa.util.websockets import ws_handler



_LOGGER = logging.getLogger("hyprxa.api.timeseries")

router = APIRouter(prefix="/timeseries", tags=["Timeseries"])


@router.get("/sources", response_model=AvailableSources, dependencies=[Depends(can_read)])
async def sources() -> AvailableSources:
    """Retrieve a list of the available data sources."""
    return {"sources": [source.source for source in _SOURCES]}


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
async def recorded(
    unitop: UnitOpDocument = Depends(map_subscriptions),
    subscriptions: AnySourceSubscriptionRequest = Depends(get_subscriptions),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="startTime"),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="endTime")
        )
    ),
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
    scan_rate: int = Query(default=5, alias="scanRate"),
    file_writer: FileWriter = Depends(get_file_writer),
) -> StreamingResponse:
    """Get a batch of recorded timeseries data."""
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
            formatter=format_timeseries_rows,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/{unitop}", response_model=SubscriptionMessage, dependencies=[Depends(can_read)])
async def samples(
    unitop: UnitOpDocument = Depends(map_subscriptions),
    data_item: str = Query(alias="dataItem"),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="startTime"),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="endTime")
        )
    ),
    limit: int = Query(default=5000),
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection)
) -> SubscriptionMessage:
    """Get timeseries data for a single subscription."""
    if limit > 15_000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Limit cannot be greater than 15000."
        )
    
    if data_item not in list(unitop.data_mapping.keys()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'{data_item}' not in '{unitop.name}'."
        )

    subscription = unitop.data_mapping[data_item]
    return await get_subscription_data(
        collection=collection,
        subscription=subscription,
        start_time=start_time,
        end_time=end_time
    )

@router.post(
    "/admin/manager/restart/{source}",
    response_model=Status,
    dependencies=[Depends(is_admin)],
    tags=["Admin"]
)
async def reset_manager(
    source: str,
    manager: TimeseriesManager = Depends(get_timeseries_manager)
) -> Status:
    """Close a timeseries manager. The next request requiring the source will
    start a new manager."""
    manager.close()
    get_timeseries_manager.invalidate(source)
    return Status(status=StatusOptions.OK)