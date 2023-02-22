import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable
from datetime import datetime
from types import TracebackType
from typing import Deque, Set, Type

import anyio

from hyprxa.base.exceptions import DroppedSubscriber
from hyprxa.base.models import BaseSubscription, SubscriberCodes, SubscriberInfo



_LOGGER = logging.getLogger("hyprxa.base.subscriber")


class BaseSubscriber:
    """Base implementation of a subscriber.
    
    Subscribers should only be created by a manager using the `subscribe` method.

    The preferred method for using a subscriber is with a context manager.
    >>> with await manager.subscribe(...) as subscriber:
    ...     async for msg in subscriber:
    ...         ...

    Subscribers can be stopped by a manager if there is an issue with the
    subscriptions or the manager's connection to RabbitMQ is lost and the manager
    cannot re-establish the connection. If that happens, the iterator is exhausted.
    Hyprxa has a `DroppedSubscriber` error for this situation...
    >>> with await manager.subscribe(...) as subscriber:
    ...     async for msg in subscriber:
    ...         ...
    ...     else:
    ...         raise DroppedSubscriber()
    """
    def __init__(self) -> None:
        self._subscriptions: Set[BaseSubscription] = set()
        self._data: Deque[str] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None

        self._created = datetime.now()
        self._total_published = 0

    @property
    def data(self) -> Deque[str]:
        """Returns the data buffer for this subscriber."""
        return self._data

    @property
    def info(self) -> SubscriberInfo:
        """Returns current information on the subscriber."""
        return SubscriberInfo(
            name=self.__class__.__name__,
            stopped=self.stopped,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self.subscriptions)
        )

    @property
    def stopped(self) -> bool:
        """Returns `True` if subscriber cannot be iterated over."""
        return self._stop_waiter is None or self._stop_waiter.done()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Returns a set of the subscriptions for this subscriber."""
        return self._subscriptions

    @property
    def stopping(self) -> asyncio.Future | None:
        """Returns the stop future which gets completed when the subscriber is
        stopped by the user or broker."""
        return self._stop_waiter

    def stop(self, e: Exception | None) -> None:
        """Stop the subscriber."""
        waiter, self._stop_waiter = self._stop_waiter, None
        if waiter is not None and not waiter.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)

    def publish(self, data: bytes) -> None:
        """Publish data to the subscriber.
        
        This method is called by the broker.
        """
        assert self._data is not None
        self._data.append(data.decode())
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        self._total_published += 1
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    def start(self, subscriptions: Set[BaseSubscription], maxlen: int) -> asyncio.Future:
        """Start the subscriber.
        
        This method is called by the broker.
        """
        assert self._stop_waiter is None
        assert self._data is None
        
        self._subscriptions.update(subscriptions)
        self._data = deque(maxlen=maxlen)
        
        waiter = asyncio.Future()
        self._stop_waiter = waiter
        return waiter

    async def wait(self) -> None:
        """Wait for new data to be published."""
        if self._data_waiter is not None:
            raise RuntimeError("Two coroutines cannot wait for data simultaneously.")
        
        if self.stopped:
            return SubscriberCodes.STOPPED
        
        stop = self._stop_waiter
        waiter = asyncio.Future()
        self._data_waiter = waiter
        try:
            await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
            if not waiter.done(): # Stop called
                _LOGGER.debug("%s stopped waiting for data", self.__class__.__name__)
                return SubscriberCodes.STOPPED
            return SubscriberCodes.DATA
        finally:
            waiter.cancel()
            self._data_waiter = None

    def __aiter__(self) -> AsyncIterable[str]:
        raise NotImplementedError()

    def __enter__(self) -> "BaseSubscriber":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None
    ) -> None:
        if isinstance(exc_value, Exception): # Not CancelledError
            self.stop(exc_value)
        else:
            self.stop(None)


async def iter_subscriber(subscriber: BaseSubscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding messages."""
    with subscriber:
        async for data in subscriber:
            yield data
        else:
            assert subscriber.stopped
            raise DroppedSubscriber()
        

async def iter_subscribers(*subscribers: BaseSubscriber) -> AsyncIterable[str]:
    """Iterates over multiple subscribers creating a single stream."""
    async def wrap_subscribers(queue: asyncio.Queue) -> None:
        async def wrap_iter_subscriber(subscriber: BaseSubscriber) -> None:
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