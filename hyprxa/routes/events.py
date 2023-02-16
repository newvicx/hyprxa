import logging
from datetime import datetime

import anyio
from fastapi import APIRouter, Depends, Query, WebSocket, status
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse

from hyprxa.auth import BaseUser
from hyprxa.base import SubscriptionError, iter_subscriber
from hyprxa.dependencies.auth import can_read, can_write
from hyprxa.dependencies.events import (
    find_one_topic,
    get_event,
    get_event_bus,
    get_event_collection,
    get_topic,
    get_topics_collection,
    list_topics,
    validate_event
)
from hyprxa.dependencies.util import get_file_writer, parse_timestamp
from hyprxa.events import (
    Event,
    EventBus,
    EventDocument,
    Topic,
    TopicDocument,
    TopicQueryResult,
    TopicSubscription,
    get_events
)
from hyprxa.util.filestream import FileWriter, chunked_transfer
from hyprxa.util.formatting import format_event_document
from hyprxa.util.status import Status, StatusOptions
from hyprxa.util.sse import sse_handler
from hyprxa.util.websockets import ws_handler



_LOGGER = logging.getLogger("hyprxa.api.events")

router = APIRouter(prefix="/events", tags=["Events"])


@router.get("/topics/{topic}", response_model=TopicDocument, dependencies=[Depends(can_read)])
async def topic(
    document: TopicDocument = Depends(get_topic)
) -> TopicDocument:
    """Retrieve a topic record."""
    return document


@router.get("/topics", response_model=TopicQueryResult, dependencies=[Depends(can_read)])
async def topics(
    documents: TopicQueryResult = Depends(list_topics)
) -> TopicQueryResult:
    """Retrieve a collection of unitop records."""
    return documents


@router.post("/topics/save", response_model=Status)
async def save(
    topic: Topic,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection),
    user: BaseUser = Depends(can_write)
) -> Status:
    """Save a topic to the database."""
    await anyio.to_thread.run_sync(find_one_topic.invalidate, topic.topic, collection)
    result = await collection.update_one(
        filter={"topic": topic.topic},
        update={
            "$set": {
                "jschema": topic.jschema,
                "modified_by": user.identity,
                "modified_at": datetime.utcnow()
            }
        },
        upsert=True
    )
    if result.modified_count > 0 or result.matched_count > 0 or result.upserted_id:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.post("/publish", response_model=Status, dependencies=[Depends(can_write)])
async def publish(
    event: Event = Depends(validate_event),
    bus: EventBus = Depends(get_event_bus),
) -> Status:
    """Publish an event to the bus."""
    if bus.publish(event.to_document()):
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.get("/stream/{topic}", response_model=Event, dependencies=[Depends(can_read)])
async def stream(
    topic: TopicDocument = Depends(get_topic),
    bus: EventBus = Depends(get_event_bus),
    routing_key: str | None = Query(default=None, alias="routingKey")
) -> EventSourceResponse:
    """Subscribe to a topic and stream events. This is an event sourcing (SSE)
    endpoint.
    """
    subscriptions = [TopicSubscription(topic=topic.topic, routing_key=routing_key)]
    subscriber = await bus.subscribe(subscriptions=subscriptions)
    send = iter_subscriber(subscriber)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{topic}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(can_read),
    topic: TopicDocument = Depends(get_topic),
    bus: EventBus = Depends(get_event_bus),
    routing_key: str | None = Query(default=None, alias="routingKey")
) -> Event:
    """Subscribe to a topic and stream events over the websocket protocol."""
    try:
        subscriptions = [TopicSubscription(topic=topic.topic, routing_key=routing_key)]
        try:
            subscriber = await bus.subscribe(subscriptions=subscriptions)
        except SubscriptionError:
            _LOGGER.warning("Refused connection due to a subscription error", exc_info=True)
            await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
            return
        except Exception:
            _LOGGER.error("Refused connection due to server error", exc_info=True)
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        else:
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


@router.get("/{topic}/recorded", response_class=StreamingResponse, dependencies=[Depends(can_read)])
async def events(
    topic: str,
    routing_key: str | None = None,
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
            formatter=format_event_document,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )