from typing import Dict, List

from fastapi import Depends, HTTPException, status
from fastapi.requests import HTTPConnection
from pydantic import Json, ValidationError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from hyprxa.base import BaseSubscriber
from hyprxa.caching import memo, singleton
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.exceptions import NotConfiguredError
from hyprxa.settings import (
    TIMESERIES_SETTINGS,
    TIMESERIES_MANAGER_SETTINGS,
    UNITOP_SETTINGS
)
from hyprxa.timeseries import (
    AnySourceSubscriptionRequest,
    Source,
    TimeseriesManager,
    UnitOp,
    UnitOpDocument,
    UnitOpQueryResult,
    ValidatedAnySourceSubscription,
    ValidatedUnitOp,
    ValidatedUnitOpDocument,
    SOURCES
)



async def get_timeseries_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the timeseries collection to perform operations against."""
    return client[TIMESERIES_SETTINGS.database_name][TIMESERIES_SETTINGS.collection_name]


async def get_unitop_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorClient:
    """Returns the unitop collection to perform operations against."""
    return client[UNITOP_SETTINGS.database_name][UNITOP_SETTINGS.collection_name]


async def get_unitop(
    unitop: str,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection),
) -> UnitOpDocument:
    """Get a unitop document by its name."""
    await find_one_unitop(unitop=unitop, _collection=collection)
    

@memo
async def find_one_unitop(unitop: str, _collection: AsyncIOMotorCollection) -> UnitOpDocument:
    """Get a unitop document by its name."""
    document = await _collection.find_one(
        {"name": unitop},
        projection={"_id": 0}
    )
    
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No unitops found matching criteria."
        )
    
    return ValidatedUnitOpDocument(**document)

async def get_unitops(
    q: Json,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection)
) -> UnitOpQueryResult:
    """Get a result set of unitops from a freeform query."""
    try:
        documents = await collection.find(q, projection={"_id": 0}).to_list(None)
    except TypeError: # Invalid query
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid query.")
    if documents:
        return UnitOpQueryResult(items=[UnitOpDocument(**document) for document in documents])
    return UnitOpQueryResult()


async def get_manager(source: Source) -> TimeseriesManager:
    """Returns a singleton instance of a manager.
    
    This method cannot be used as dependency. Use `get_subscribers` instead.
    """
    return singleton(TIMESERIES_MANAGER_SETTINGS.get_manager)(source)


async def get_subscriptions(
    connection: HTTPConnection,
    unitop: UnitOpDocument = Depends(get_unitop)
) -> AnySourceSubscriptionRequest:
    """Extract subscriptions from unitop and authorize all sources."""
    subscriptions = AnySourceSubscriptionRequest(
        subscriptions=[subscription for subscription in unitop.data_mapping.values()]
    )

    groups = subscriptions.group()
    for source_id in groups.keys():
        if source_id not in SOURCES:
            # The source might no longer be used. In which case the unitop needs
            # to be updated to remove it.
            raise NotConfiguredError(f"{source_id} is not registered with application.")
        source = SOURCES[source_id]
        await source.is_authorized(connection)
    return subscriptions


async def get_subscribers(
    subscriptions: AnySourceSubscriptionRequest = Depends(get_subscriptions)
) -> List[BaseSubscriber]:
    """Create subscribers and subscribe to all sources in a unitop."""
    groups = subscriptions.group()
    managers: Dict[str, TimeseriesManager] = {}
    for source_id in groups.keys():
        source = SOURCES[source_id]
        manager = await get_manager(source)
        managers[source.source] = manager
    
    subscribers: List[BaseSubscriber] = []
    for source_id, subscriptions in groups.items():
        manager = managers[source_id]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers


async def validate_sources(unitop: UnitOp) -> UnitOp:
    """Validate that sources in unitop data mapping are valid."""
    subscription_model = ValidatedAnySourceSubscription()
    unitop_model = ValidatedUnitOp(subscription_model)
    try:
        unitop_model(
            name=unitop.name,
            data_mapping=unitop.data_mapping,
            meta=unitop.meta
        )
    except ValidationError as e:
        # If we just re-raise, FastAPI will consider it a 500 error. We want 422
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=e.errors()
        ) from e
    else:
        return unitop
    