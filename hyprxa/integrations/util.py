import asyncio
from collections.abc import AsyncIterable

import anyio

from hyprxa.integrations.exceptions import DroppedSubscriber
from hyprxa.integrations.protocols import Subscriber



async def iter_subscriber(subscriber: Subscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding events."""
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