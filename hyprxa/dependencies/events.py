from dataclasses import astuple

from fastapi import Depends, HTTPException, status
from jsonschema import ValidationError, validate
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from hyprxa.caching import memo, singleton
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.events import (
    Event,
    EventBus,
    EventDocument,
    TopicDocument,
    TopicQueryResult,
    ValidatedTopicDocument,
    ValidatedEventDocument
)
from hyprxa.settings import EVENT_SETTINGS, EVENT_BUS_SETTINGS, TOPIC_SETTINGS



async def get_event_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the event collection to perform operations against."""
    return client[EVENT_SETTINGS.database_name][EVENT_SETTINGS.collection_name]


async def get_topics_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the topics collection to perform operations against."""
    return client[TOPIC_SETTINGS.database_name][TOPIC_SETTINGS.collection_name]


async def get_event(
    topic: str,
    routing_key: str | None = None,
    collection: AsyncIOMotorCollection = Depends(get_event_collection)
) -> EventDocument:
    """Get the most recent event matching both topic and routing key.
    
    If routing_key is None, returns the most recent event for the topic.
    """
    return await find_one_event(
        collection=collection,
        topic=topic,
        routing_key=routing_key
    )


async def find_one_event(
    collection: AsyncIOMotorCollection,
    topic: str,
    routing_key: str | None = None,
) -> EventDocument:
    """Get the most recent event matching both topic and routing key.
    
    If routing_key is None, returns the most recent event for the topic.
    """
    query = {"topic": topic}
    if routing_key:
        query.update({"events.routing_key": routing_key})
    
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
    
    documents = [ValidatedEventDocument(**event) for event in events["events"]]
    if routing_key:
        documents = [document for document in documents if document.routing_key == routing_key]

    if not documents:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No events found matching criteria."
        )
    
    index = sorted([(document.timestamp, i) for i, document in enumerate(documents)])
    return documents[index[-1][1]]


async def get_topic(
    topic: str,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicDocument:
    """Search for a topic by name."""
    return await find_one_topic(topic=topic, _collection=collection)


# Topics need to be recalled frequently on event publishing so we cache the
# result and invalidate it on an update
@memo
async def find_one_topic(topic: str, _collection: AsyncIOMotorCollection) -> TopicDocument:
    """Search for a topic by name."""
    document = await _collection.find_one({"topic": topic}, projection={"_id": 0})
    if document:
        return ValidatedTopicDocument(**document)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No topics found matching criteria."
    )


async def list_topics(
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicQueryResult:
    """List all topics in the database."""
    documents = await collection.find(projection={"topic": 1, "_id": 0}).to_list(None)
    if documents:
        return TopicQueryResult(items=[document["topic"] for document in documents])
    return TopicQueryResult()


async def validate_event(
    event: Event,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> Event:
    """Validate an event payload against the topic schema."""
    document = await find_one_topic(topic=event.topic, _collection=collection)
    try:
        validate(event.payload, document.jschema)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{e.json_path}-{e.message}"
        )
    return event


async def get_event_bus() -> EventBus:
    """Returns a singleton instance of an event bus."""
    return singleton(EVENT_BUS_SETTINGS.get_event_bus)()