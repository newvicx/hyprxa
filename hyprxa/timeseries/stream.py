import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Any, Dict, List, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.timeseries.models import AnySourceSubscriptionRequest
from hyprxa.types import TimeseriesRow
from hyprxa.util.time import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range_on_frequency
)



def format_timeseries_content(
    content: List[Dict[str, datetime | Any]]
) -> Dict[str, List[datetime | Any]]:
    """Format query results for iteration."""
    formatted = {"timestamp": [], "value": []}
    for item in content:
        formatted["timestamp"].append(item["timestamp"])
        formatted["value"].append(item["value"])
    return formatted


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
        database_name: The database to query.
        collection_name: The collection to query.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query.
        scan_rate: A representative number of the data update frequency.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

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
        dispatch = [
            collection.find(
                filter={
                    "timestamp": {"$gte": start_time, "$lt": end_time},
                    "subscription": hash_,
                    "source": source
                },
                projection={"timestamp": 1, "value": 1, "_id": 0}
            ).sort("timestamp", 1).to_list(None) for hash_, source in hashes
        ]
        contents = await asyncio.gather(*dispatch)
        data = [format_timeseries_content(content) for content in contents]
        index = get_timestamp_index(data)

        for timestamp, row in iter_timeseries_rows(index, data):
            yield timestamp, row
    else:
        # This covers the inclusion of the end time in the query range
        dispatch = [
            collection.find(
                filter={
                    "timestamp": end_time,
                    "subscription": hash_
                },
                projection={"timestamp": 1, "value": 1, "_id": 0}
            ).sort("timestamp", 1).to_list(None) for hash_ in hashes
        ]
        contents = await asyncio.gather(*dispatch)
        data = [format_timeseries_content(content) for content in contents]
        index = get_timestamp_index(data)
        
        if index:
            for timestamp, row in iter_timeseries_rows(index, data):
                yield timestamp, row