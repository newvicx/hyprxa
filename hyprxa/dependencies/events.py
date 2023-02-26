import re
from typing import List

from fastapi import Depends, HTTPException, Query, status
from jsonschema import ValidationError, validate
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from hyprxa.caching.singleton import singleton
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.dependencies.topics import find_topic, get_topics_collection
from hyprxa.events.manager import EventManager
from hyprxa.events.models import (
    Event,
    EventDocument,
    EventQueryResult,
    ValidatedEventDocument
)
from hyprxa.settings import EVENT_SETTINGS, EVENT_BUS_SETTINGS
from hyprxa.topics.models import Topic



async def get_event_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the event collection to perform operations against."""
    return client[EVENT_SETTINGS.database_name][EVENT_SETTINGS.collection_name]


async def get_event(
    topic: str,
    routing_key: str | None = Query(default=None, alias="routingKey"),
    collection: AsyncIOMotorCollection = Depends(get_event_collection)
) -> EventDocument:
    """Get the most recent event matching both topic and routing key.
    
    If routing_key is `None`, returns the most recent event for the topic. The
    routing key could be anything.
    """
    query = {"topic": topic}
    if routing_key:
        query.update({"routing_key": routing_key})
    
    events = await collection.find_one(
        query,
        projection={"events": 1, "_id": 0},
        sort=[("timestamp", -1)]
    )

    if not events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No events found matching criteria."
        )
    
    documents: List[EventDocument] = [ValidatedEventDocument(**event) for event in events["events"]]

    if not documents:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No events found matching criteria."
        )
    
    index = sorted([(document.timestamp, i) for i, document in enumerate(documents)])
    return documents[index[-1][1]]


async def match_events(
    topic: str,
    routing_key: str = Query(alias="routingKey"),
    collection: AsyncIOMotorCollection = Depends(get_event_collection)
) -> EventQueryResult:
    """Return the last posted event for a topic where all routing keys match
    the pattern.
    """
    regex = re.compile(routing_key, re.IGNORECASE)
    query = {"topic": topic, "routing_key": {"$regex": regex}}
    
    keys = await collection.find(query).distinct("routing_key")
    
    events = []
    for key in keys:
        try:
            event = await get_event(topic=topic, routing_key=key, collection=collection)
        except HTTPException:
            pass
        else:
            events.append(event)

    return EventQueryResult(items=events)

async def validate_event(
    event: Event,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> Event:
    """Validate an event payload against the topic schema."""
    topic: Topic = await find_topic(topic=event.topic, _collection=collection)
    try:
        validate(event.payload, topic.jschema)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{e.json_path}-{e.message}"
        )
    return event


@singleton
async def get_event_manager() -> EventManager:
    """Returns a singleton instance of an event manager."""
    return await EVENT_BUS_SETTINGS.get_manager()