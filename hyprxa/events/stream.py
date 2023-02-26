import re
import json
from collections.abc import AsyncIterable
from datetime import datetime
from typing import List, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.events.models import EventDocument, ValidatedEventDocument
from hyprxa.types import JSONPrimitive



async def get_events(
    collection: AsyncIOMotorCollection,
    topic: str,
    start_time: datetime,
    end_time: datetime | None = None,
    routing_key: str | None = None,
) -> AsyncIterable[Tuple[JSONPrimitive, ...]]:
    """Stream events matching both topic and routing key in a time range.
    
    Args:
        collection: The motor collection.
        topic: The event topic to search.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query. This is inclusive.
        routing_key: The routing key pattern for events to query.

    Yields:
        row: An event row.

    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.utcnow().replace(tzinfo=None)
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    query = {"topic": topic, "timestamp": {"$gte": start_time, "$lte": end_time}}
    if routing_key:
        regex = re.compile(routing_key, re.IGNORECASE)
        query.update({"routing_key": {"$regex": regex}})
    
    async for events in collection.find(
        query,
        projection={"events": 1, "_id": 0}
    ).sort("timestamp", 1):
        if events:
            documents: List[EventDocument] = [
                ValidatedEventDocument(**event) for event in events["events"]
            ]
            if documents:
                index = sorted([(document.timestamp, i) for i, document in enumerate(documents)])
                for timestamp, i in index:
                    if timestamp >= start_time and timestamp <= end_time:
                        document = documents[i]
                        payload = json.dumps(document.payload)
                        row = (
                            timestamp.isoformat(),
                            document.posted_by,
                            document.topic,
                            document.routing_key,
                            payload
                        )
                        yield row