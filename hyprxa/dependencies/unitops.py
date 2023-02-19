from collections.abc import Sequence
from enum import Enum
from typing import Dict

from fastapi import Depends, HTTPException, status
from pydantic import Json
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from pydantic import ValidationError, create_model, validator

from hyprxa.caching.memo import memo
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.settings import UNITOP_SETTINGS
from hyprxa.timeseries.models import (
    AnySourceSubscription,
    AnySourceSubscriptionRequest
)
from hyprxa.timeseries.sources import _SOURCES
from hyprxa.unitops.models import (
    UnitOp,
    UnitOpDocument,
    UnitOpQueryResult,
    ValidatedUnitOpDocument
)


def source_to_str(cls, v: Enum) -> str:
    """Convert source enum to string."""
    return v.value


ValidatedAnySourceSubscription = lambda: create_model(
    "AnySourceSubscription",
    source=(_SOURCES.compile_sources(), ...),
    __base__=AnySourceSubscription,
    __validators__={"_source_converter": validator("source", allow_reuse=True)(source_to_str)}
)
ValidatedAnySourceSubscriptionRequest = lambda model: create_model(
    "AnySourceSubscriptionRequest",
    subscriptions=(Sequence[model], ...),
    __base__=AnySourceSubscriptionRequest
)
ValidatedUnitOp = lambda model: create_model(
    "UnitOp",
    data_mapping=(Dict[str, model], ...),
    __base__=UnitOp
)


async def get_unitop_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorClient:
    """Returns the unitop collection to perform operations against."""
    return client[UNITOP_SETTINGS.database_name][UNITOP_SETTINGS.collection_name]


@memo
async def get_unitop(
    unitop: str,
    _collection: AsyncIOMotorCollection = Depends(get_unitop_collection),
) -> UnitOpDocument:
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