from fastapi import Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from hyprxa.caching.memo import memo
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.settings import TOPIC_SETTINGS
from hyprxa.topics.models import (
    TopicDocument,
    TopicQueryResult,
    ValidatedTopicDocument
)



async def get_topics_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the topics collection to perform operations against."""
    return client[TOPIC_SETTINGS.database_name][TOPIC_SETTINGS.collection_name]


async def get_topic(
    topic: str,
    _collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicDocument:
    """Search for a topic by name."""
    return await find_topic(
        topic=topic,
        _collection=_collection
    )


@memo
async def find_topic(
    topic: str,
    _collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicDocument:
    document = await _collection.find_one({"topic": topic}, projection={"_id": 0})
    if document:
        return ValidatedTopicDocument(**document)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No topics found matching criteria."
    )


@memo
async def list_topics(
    _collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicQueryResult:
    """List all topics in the database."""
    documents = await _collection.find(projection={"topic": 1, "_id": 0}).to_list(None)
    if documents:
        return TopicQueryResult(items=[document["topic"] for document in documents])
    return TopicQueryResult()