from typing import Dict, List

from fastapi import Depends, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from starlette.requests import HTTPConnection

from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.caching.singleton import singleton
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.dependencies.unitops import map_subscriptions
from hyprxa.settings import TIMESERIES_SETTINGS, TIMESERIES_MANAGER_SETTINGS
from hyprxa.timeseries.manager import TimeseriesManager
from hyprxa.timeseries.models import AnySourceSubscriptionRequest
from hyprxa.timeseries.sources import _SOURCES
from hyprxa.unitops.models import UnitOpDocument
from hyprxa._exceptions import NotConfiguredError



@singleton
async def get_timeseries_manager(source: str) -> TimeseriesManager:
    """Returns a singleton instance of a timeseries manager."""
    if source not in _SOURCES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{source} is not a registered source."
        )
    source = _SOURCES[source]
    return await TIMESERIES_MANAGER_SETTINGS.get_manager(source)


async def get_timeseries_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the timeseries collection to perform operations against."""
    return client[TIMESERIES_SETTINGS.database_name][TIMESERIES_SETTINGS.collection_name]


async def get_subscriptions(
    connection: HTTPConnection,
    unitop: UnitOpDocument = Depends(map_subscriptions),
    data_items: List[str] = Query(default=None, alias="dataItem")
) -> AnySourceSubscriptionRequest:
    """Extract subscriptions from unitop and authorize all sources."""
    data_items = set(data_items or list(unitop.data_mapping.keys()))
    
    subscriptions=[
        unitop.data_mapping[data_item] for data_item in data_items if data_item in unitop.data_mapping
    ]
    if not subscriptions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No subscriptions matching criteria."
        )
    
    subscriptions = AnySourceSubscriptionRequest(subscriptions=subscriptions)
    groups = subscriptions.group()
    for source_ in groups.keys():
        if source_ not in _SOURCES:
            # The source might no longer be used. In which case the unitop needs
            # to be updated to remove it.
            raise NotConfiguredError(f"{source_} is not registered with application.")
        source = _SOURCES[source_]
        await source.is_authorized(connection)
    return subscriptions


async def get_subscribers(
    subscriptions: AnySourceSubscriptionRequest = Depends(get_subscriptions)
) -> List[BaseSubscriber]:
    """Create subscribers and subscribe to all sources in a unitop."""
    groups = subscriptions.group()
    managers: Dict[str, TimeseriesManager] = {}
    for source in groups.keys():
        manager = await get_timeseries_manager(source)
        managers[source] = manager
    
    subscribers: List[BaseSubscriber] = []
    for source, subscriptions in groups.items():
        manager = managers[source]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers
    