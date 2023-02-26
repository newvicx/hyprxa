import logging
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse
from starlette.websockets import WebSocket

from hyprxa.auth import BaseUser
from hyprxa.base import iter_subscriber
from hyprxa.dependencies.auth import can_read, can_write, is_admin
from hyprxa.dependencies.events import (
    get_event,
    get_event_collection,
    get_event_manager,
    match_events,
    validate_event
)
from hyprxa.dependencies.topics import get_topic
from hyprxa.dependencies.util import get_file_writer, parse_timestamp
from hyprxa.events.manager import EventManager
from hyprxa.events.models import Event, EventDocument, EventQueryResult
from hyprxa.events.stream import get_events
from hyprxa.topics.models import (
    TopicDocument,
    TopicSubscription
)
from hyprxa.util.filestream import FileWriter, chunked_transfer
from hyprxa.util.status import Status, StatusOptions
from hyprxa.util.sse import sse_handler
from hyprxa.util.websockets import ws_handler



_LOGGER = logging.getLogger("hyprxa.api.events")

router = APIRouter(prefix="/events", tags=["Events"])


@router.post("/publish", response_model=Status, dependencies=[Depends(can_write)])
async def publish(
    event: Event = Depends(validate_event),
    manager: EventManager = Depends(get_event_manager),
) -> Status:
    """Publish an event to the manager."""
    if manager.publish(event.to_document()):
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.get("/stream/{topic}", response_class=EventSourceResponse, dependencies=[Depends(can_read)])
async def stream(
    topic: TopicDocument = Depends(get_topic),
    manager: EventManager = Depends(get_event_manager),
    routing_key: str | None = Query(default=None, alias="routingKey")
) -> EventSourceResponse:
    """Subscribe to a topic and stream events. This is an event sourcing (SSE)
    endpoint.
    """
    routing_key = routing_key.replace("$", "#")
    subscriptions = [TopicSubscription(topic=topic.topic, routing_key=routing_key)]
    subscriber = await manager.subscribe(subscriptions=subscriptions)
    send = iter_subscriber(subscriber)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{topic}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(can_read),
    topic: TopicDocument = Depends(get_topic),
    manager: EventManager = Depends(get_event_manager),
    routing_key: str | None = Query(default=None, alias="routingKey")
) -> Event:
    """Subscribe to a topic and stream events over the websocket protocol."""
    subscriptions = [TopicSubscription(topic=topic.topic, routing_key=routing_key)]
    subscriber = await manager.subscribe(subscriptions=subscriptions)
    try:
        await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        subscriber.stop()
        return
    send = iter_subscriber(subscriber)
    await ws_handler(websocket, _LOGGER, None, send)


@router.get("/{topic}/last", response_model=EventDocument, dependencies=[Depends(can_read)])
async def event(document: EventDocument = Depends(get_event)) -> EventDocument:
    """Get last event for a topic-routing key combination."""
    return document


@router.get("/{topic}/query", response_model=EventQueryResult, dependencies=[Depends(can_read)])
async def query(
    documents: EventQueryResult = Depends(match_events)
) -> EventQueryResult:
    """Return the last posted event for a topic where all routing keys match
    the pattern.
    """
    return documents.dict()


@router.get("/{topic}/recorded", response_class=StreamingResponse, dependencies=[Depends(can_read)])
async def recorded(
    topic: str,
    routing_key: str | None = Query(default=None, alias="routingKey"),
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
    collection: AsyncIOMotorCollection = Depends(get_event_collection),
    file_writer: FileWriter = Depends(get_file_writer),
) -> StreamingResponse:
    """Get a batch of recorded events."""
    send = get_events(
        collection=collection,
        topic=topic,
        start_time=start_time,
        end_time=end_time,
        routing_key=routing_key
    )

    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )

    chunk_size = 1000
    writer(["timestamp", "posted_by", "topic", "routing_key", "payload"])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-events{suffix}"
    )

    return StreamingResponse(
        chunked_transfer(
            send=send,
            buffer=buffer,
            writer=writer,
            formatter=None,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.post(
    "/admin/manager/restart",
    response_model=Status,
    dependencies=[Depends(is_admin)],
    tags=["Admin"]
)
async def reset_manager(manager: EventManager = Depends(get_event_manager)) -> Status:
    """Close a manager. The next request requiring the source will start a
    new manager."""
    manager.close()
    get_event_manager.invalidate()
    return Status(status=StatusOptions.OK)