from datetime import datetime

import anyio
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.auth import BaseUser
from hyprxa.dependencies.auth import can_read, can_write
from hyprxa.dependencies.unitops import (
    get_unitop,
    get_unitop_collection,
    get_unitops,
    validate_sources
)
from hyprxa.unitops.models import UnitOp, UnitOpDocument, UnitOpQueryResult
from hyprxa.util.status import Status, StatusOptions



router = APIRouter(prefix="/unitops", tags=["Unitops"])


@router.get("/search/{unitop}", response_model=UnitOpDocument, dependencies=[Depends(can_read)])
async def unitop(
    document: UnitOpDocument = Depends(get_unitop)
) -> UnitOpDocument:
    """Retrieve a unitop record."""
    return document


@router.get("/search", response_model=UnitOpQueryResult, dependencies=[Depends(can_read)])
async def unitops(
    unitops: UnitOpQueryResult = Depends(get_unitops)
) -> UnitOpQueryResult:
    """Retrieve a collection of unitop records."""
    return unitops


@router.post("/save", response_model=Status)
async def save(
    unitop: UnitOp = Depends(validate_sources),
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection),
    user: BaseUser = Depends(can_write)
) -> Status:
    """Save a topic to the database."""
    data_mapping = unitop.dict()["data_mapping"]
    result = await collection.update_one(
        filter={"name": unitop.name},
        update={
            "$set": {
                "data_mapping": data_mapping,
                "meta": unitop.meta,
                "modified_by": user.identity,
                "modified_at": datetime.utcnow()
            }
        },
        upsert=True
    )
    if result.modified_count > 0:
        await anyio.to_thread.run_sync(get_unitop.invalidate, unitop.name, collection)
        return Status(status=StatusOptions.OK)
    elif result.matched_count > 0 or result.upserted_id:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)