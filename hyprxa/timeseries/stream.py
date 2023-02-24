from collections.abc import AsyncIterable
from datetime import datetime
from typing import Any, Dict, List, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.timeseries.models import (
    AnySourceSubscription,
    AnySourceSubscriptionRequest,
    SubscriptionMessage
)
from hyprxa.types import TimeseriesRow
from hyprxa.util.asyncutils import create_gather_task_group
from hyprxa.util.time import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range_on_frequency
)



def format_timeseries_content(
    content: List[Dict[str, datetime | Any] | None]
) -> Dict[str, List[datetime | Any]]:
    """Format query results for iteration."""
    formatted = {"timestamp": [], "value": []}
    for item in content:
        formatted["timestamp"].append(item["timestamp"])
        formatted["value"].append(item["value"])
    return formatted


async def find_timeseries_documents_in_range(
    collection: AsyncIOMotorCollection,
    start_time: datetime,
    end_time: datetime,
    hash_: int,
    source: str
) -> List[Dict[str, datetime | Any] | None]:
    """Find all timeseries documents for given hash and source in a time range."""
    return await collection.find(
        filter={
            "timestamp": {"$gte": start_time, "$lt": end_time},
            "subscription": hash_,
            "source": source
        },
        projection={"timestamp": 1, "value": 1, "_id": 0}
    ).sort("timestamp", 1).to_list(None)


async def find_timeseries_documents_at_time(
    collection: AsyncIOMotorCollection,
    time: datetime,
    hash_: int,
    source: str
) -> List[Dict[str, datetime | Any] | None]:
    """Find a timeseries document for given hash and source at a specific time."""
    return await collection.find(
        filter={
            "timestamp": time,
            "subscription": hash_,
            "source": source
        },
        projection={"timestamp": 1, "value": 1, "_id": 0}
    ).sort("timestamp", 1).to_list(1)


async def get_timeseries(
    collection: AsyncIOMotorCollection,
    subscriptions: AnySourceSubscriptionRequest,
    start_time: datetime,
    end_time: datetime | None = None,
    scan_rate: int = 5
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned data for a subscription request.
    
    The subscriptions are sorted according to their hash. Row indices align
    with the hash order.

    Args:
        collection: The motor collection.
        subscriptions: The subscriptions to stream data for.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query. This is inclusive.
        scan_rate: A representative number of the data update frequency.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.utcnow()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    # Factory into the time range splitting to make sure we dont load too many
    # documents into memory
    request_chunk_size = min(int(150_000/len(subscriptions.subscriptions)), 10_000)
    
    start_times, end_times = split_range_on_frequency(
        start_time=start_time,
        end_time=end_time,
        request_chunk_size=request_chunk_size,
        scan_rate=scan_rate
    )

    groups = subscriptions.group()
    subscriptions: List[Tuple[int, str]] = []
    for source in groups:
        for subscription in groups[source]:
            subscriptions.append((hash(subscription), source))
    hashes = sorted(subscriptions)

    for start_time, end_time in zip(start_times, end_times):
        keys = []
        async with create_gather_task_group() as tg:
            for hash_, source in hashes:
                key = tg.start_soon(
                    find_timeseries_documents_in_range,
                    collection,
                    start_time,
                    end_time,
                    hash_,
                    source
                )
                keys.append(key)

        contents = [tg.get_result(key) for key in keys]
        data = [format_timeseries_content(content) for content in contents]
        index = get_timestamp_index(data)

        for timestamp, row in iter_timeseries_rows(index, data):
            yield timestamp, row
    
    # The query for documents in a range specifies `$lt: end_time` so that we
    # do not pull duplicate documents as we iterate through the start and end
    # times (we use $gte: start). So after we have gone through the whole time
    # range we make one last query for the end time so the range is inclusive.
    else:
        keys = []
        async with create_gather_task_group() as tg:
            for hash_, source in hashes:
                key = tg.start_soon(
                    find_timeseries_documents_at_time,
                    collection,
                    end_time,
                    hash_,
                    source
                )
        
        contents = [tg.get_result(key) for key in keys]
        data = [format_timeseries_content(content) for content in contents]
        index = get_timestamp_index(data)
        
        # This works fine even if there are no samples (i.e index is empty list)
        for timestamp, row in iter_timeseries_rows(index, data):
            yield timestamp, row


async def get_subscription_data(
    collection: AsyncIOMotorCollection,
    subscription: AnySourceSubscription,
    start_time: datetime,
    end_time: datetime | None = None,
    limit: int | None = None
) -> SubscriptionMessage:
    """Retrieve timeseries data for a subscription in a time range.
    
    Args:
        collection: The motor collection.
        subscription: The subscription to get data for.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query. This is inclusive.
        scan_rate: A representative number of the data update frequency.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.utcnow()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    limit = limit or 5000

    samples: List[Dict[str, datetime | Any]] = await collection.find(
        filter={
            "timestamp": {"$gte": start_time, "$lte": end_time},
            "subscription": hash(subscription),
            "source": subscription.source
        },
        projection={"timestamp": 1, "value": 1, "_id": 0}
    ).sort("timestamp", 1).to_list(limit)

    return SubscriptionMessage(
        subscription=subscription,
        items=[{"timestamp": sample["timestamp"], "value": sample["value"]} for sample in samples]
    )