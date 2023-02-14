import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Any, Dict, List, Set

from motor.motor_asyncio import AsyncIOMotorCollection

from hyprxa.integrations import Subscription
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
    subscriptions: Set[Subscription],
    start_time: datetime,
    end_time: datetime | None = None,
    scan_rate: int = 5
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned data for a sequence of subscriptions.
    
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

    request_chunk_size = min(int(150_000/len(subscriptions)), 10_000)
    hashes = [hash(subscription) for subscription in sorted(subscriptions)]
    start_times, end_times = split_range_on_frequency(
        start_time=start_time,
        end_time=end_time,
        request_chunk_size=request_chunk_size,
        scan_rate=scan_rate
    )

    for start_time, end_time in zip(start_times, end_times):
        dispatch = [
            collection.find(
                filter={
                    "timestamp": {"$gte": start_time, "$lt": end_time},
                    "subscription": hash_
                },
                projection={"timestamp": 1, "value": 1, "_id": 0}
            ).sort("timestamp", 1).to_list(None) for hash_ in hashes
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


async def iter_subscriber(subscriber: Subscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding messages."""
    with subscriber:
        async for data in subscriber:
            yield data
        else:
            assert subscriber.stopped
            raise DroppedSubscriber()
        

async def iter_subscribers(*subscribers: Subscriber) -> AsyncIterable[str]:
    """Iterates over multiple subscribers creating a single stream."""
    async def wrap_subscribers(queue: asyncio.Queue) -> None:
        async def wrap_iter_subscriber(subscriber: Subscriber) -> None:
            async for data in iter_subscriber(subscriber):
                await queue.put(data)
        
        async with anyio.create_task_group() as tg:
            for subscriber in subscribers:
                tg.start_soon(wrap_iter_subscriber, subscriber)
        
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue(maxsize=1000)
    wrapper = loop.create_task(wrap_subscribers(queue))
    try:
        while True:
            getter = loop.create_task(queue.get())
            await asyncio.wait([getter, wrapper], return_when=asyncio.FIRST_COMPLETED)
            if not getter.done():
                assert wrapper.done()
                e = wrapper.exception()
                if e:
                    raise e
                else:
                    raise DroppedSubscriber()
            yield getter.result()
    finally:
        getter.cancel()
        wrapper.cancel()