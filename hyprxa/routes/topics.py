from datetime import datetime

import anyio
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.auth.models import BaseUser
from hyprxa.dependencies.auth import can_read, can_write
from hyprxa.dependencies.topics import (
    get_topic,
    get_topics_collection,
    list_topics
)
from hyprxa.topics.models import (
    Topic,
    TopicDocument,
    TopicQueryResult
)
from hyprxa.util.status import Status, StatusOptions



router = APIRouter(prefix="/topics", tags=["Topics"])


@router.get("/search/{topic}", response_model=TopicDocument, dependencies=[Depends(can_read)])
async def topic(
    document: TopicDocument = Depends(get_topic)
) -> TopicDocument:
    """Retrieve a topic record."""
    return document


@router.get("/search", response_model=TopicQueryResult, dependencies=[Depends(can_read)])
async def topics(
    documents: TopicQueryResult = Depends(list_topics)
) -> TopicQueryResult:
    """Retrieve a collection of unitop records."""
    return documents


@router.post("/save", response_model=Status)
async def save(
    topic: Topic,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection),
    user: BaseUser = Depends(can_write)
) -> Status:
    """Save a topic to the database."""
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
    if result.modified_count > 0:
        await anyio.to_thread.run_sync(get_topic.invalidate, topic.topic, collection)
        await anyio.to_thread.run_sync(list_topics.invalidate, collection)
        return Status(status=StatusOptions.OK)
    elif result.upserted_id:
        await anyio.to_thread.run_sync(list_topics.invalidate, collection)
        return Status(status=StatusOptions.OK)
    elif result.matched_count > 0:
        return Status(status=StatusOptions.OK)
    
    return Status(status=StatusOptions.FAILED)