from typing import Dict, List

from fastapi import Depends, HTTPException, status
from fastapi.requests import HTTPConnection
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.caching.singleton import singleton
from hyprxa.dependencies.db import get_mongo_client
from hyprxa.dependencies.unitops import get_unitop
from hyprxa.exceptions import NotConfiguredError
from hyprxa.settings import TIMESERIES_SETTINGS, TIMESERIES_MANAGER_SETTINGS
from hyprxa.timeseries.manager import TimeseriesManager
from hyprxa.timeseries.models import AnySourceSubscriptionRequest
from hyprxa.timeseries.sources import Source, _SOURCES
from hyprxa.unitops.models import UnitOpDocument



@singleton
async def get_manager(source: Source) -> TimeseriesManager:
    """Returns a singleton instance of a manager.
    
    This method should not be used as dependency in a path operation. Use
    `get_subscribers`, or `get_manager_dependency` instead.
    """
    return await TIMESERIES_MANAGER_SETTINGS.get_manager(source)


async def get_manager_dependency(source: str) -> TimeseriesManager:
    if source not in _SOURCES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{source} is not a registered source."
        )
    source = _SOURCES[source]
    return await get_manager(source)


async def get_timeseries_collection(
    client: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the timeseries collection to perform operations against."""
    return client[TIMESERIES_SETTINGS.database_name][TIMESERIES_SETTINGS.collection_name]


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
        if source_id not in _SOURCES:
            # The source might no longer be used. In which case the unitop needs
            # to be updated to remove it.
            raise NotConfiguredError(f"{source_id} is not registered with application.")
        source = _SOURCES[source_id]
        await source.is_authorized(connection)
    return subscriptions


async def get_subscribers(
    subscriptions: AnySourceSubscriptionRequest = Depends(get_subscriptions)
) -> List[BaseSubscriber]:
    """Create subscribers and subscribe to all sources in a unitop."""
    groups = subscriptions.group()
    managers: Dict[str, TimeseriesManager] = {}
    for source_id in groups.keys():
        source = _SOURCES[source_id]
        manager = await get_manager(source)
        managers[source.source] = manager
    
    subscribers: List[BaseSubscriber] = []
    for source_id, subscriptions in groups.items():
        manager = managers[source_id]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers
    