from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.caching import memo
from hyprxa.events.models import EventDocument, ValidatedEventDocument



@memo
async def find_one_event(
    _collection: AsyncIOMotorCollection,
    topic: str,
    routing_key: str | None = None,
) -> EventDocument:
    """Get the most recent event matching both topic and routing key.
    
    If routing_key is None, returns the most recent event for the topic.
    """
    query = {"topic": topic}
    if routing_key:
        query.update({"events.routing_key": routing_key})
    
    events = await _collection.find_one(
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