import asyncio
import hashlib
import logging
from collections import deque
from collections.abc import AsyncIterable
from contextlib import suppress
from datetime import datetime
from types import TracebackType
from typing import Any, Deque, Dict, Set, Type

from hyprxa.integrations.exceptions import ClientClosed
from hyprxa.integrations.models import (
    ClientInfo,
    ConnectionInfo,
    DroppedSubscriptions,
    SubscriberCodes,
    SubscriberInfo,
    Subscription,
    SubscriptionMessage
)
from hyprxa.integrations.protocols import (
    Client,
    Connection,
    Lock,
    Subscriber
)



_LOGGER = logging.getLogger("hyprxa.integrations")


class BaseClient(Client):
    """Base implementation for a client.
    
    Args:
        max_buffered_messages: The max length of the data queue for the client.
    """
    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self._connections: Dict[asyncio.Task, Connection] = {}
        self._data: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self._dropped: asyncio.Queue = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._connections_serviced = 0

    @property
    def capacity(self) -> int:
        raise NotImplementedError()

    @property
    def closed(self) -> bool:
        raise NotImplementedError()

    @property
    def info(self) -> ClientInfo:
        return ClientInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            data_queue_size=self._data.qsize(),
            dropped_connection_queue_size=self._dropped.qsize(),
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            active_connections=len(self._connections),
            active_subscriptions=len(self.subscriptions),
            subscription_capacity=self.capacity,
            total_connections_serviced=self._connections_serviced,
            connection_info=[connection.info for connection in self._connections.values()]
        )

    @property
    def subscriptions(self) -> Set[Subscription]:
        subscriptions = set()
        for fut, connection in self._connections.items():
            if not fut.done():
                subscriptions.update(connection.subscriptions)
        return subscriptions

    def clear(self) -> None:
        for queue in (self._data, self._dropped):
            try:
                while True:
                    queue.get_nowait()
                    queue.task_done()
            except asyncio.QueueEmpty:
                pass

    async def close(self) -> None:
        raise NotImplementedError()

    async def dropped(self) -> AsyncIterable[DroppedSubscriptions]:
        while not self.closed:
            msg = await self._dropped.get()
            self._dropped.task_done()
            yield msg
        else:
            raise ClientClosed()
    
    async def messages(self) -> AsyncIterable[SubscriptionMessage]:
        while not self.closed:
            msg = await self._data.get()
            self._data.task_done()
            yield msg
        else:
            raise ClientClosed()

    async def subscribe(self, subscriptions: Set[Subscription]) -> bool:
        raise NotImplementedError()

    async def unsubscribe(self, subscriptions: Set[Subscription]) -> bool:
        raise NotImplementedError()

    def connection_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._connections
        connection = self._connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        # If a connection was cancelled by the client and the subscriptions were
        # replaced through another connection, subscriptions will be empty set
        msg = DroppedSubscriptions(
            subscriptions=connection.subscriptions.difference(self.subscriptions),
            error=e
        )
        self._dropped.put_nowait(msg)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class BaseConnection(Connection):
    """Base implementation for a connection."""
    def __init__(self) -> None:
        self._subscriptions: Set[Subscription] = set()
        self._data: asyncio.Queue = None
        self._online: bool = False
        self._started: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._total_published = 0

    @property
    def info(self) -> ConnectionInfo:
        return ConnectionInfo(
            name=self.__class__.__name__,
            online=self.online,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self._subscriptions)
        )

    @property
    def online(self) -> bool:
        return self._online

    @property
    def subscriptions(self) -> Set[Subscription]:
        return self._subscriptions

    def toggle(self) -> None:
        self._online = not self._online

    async def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def start(
        self,
        subscriptions: Set[Subscription],
        data: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> asyncio.Task:
        raise NotImplementedError()


class BaseSubscriber(Subscriber):
    """Base implementation for a subscriber."""
    def __init__(self) -> None:
        self._subscriptions = set()
        self._data: Deque[str | bytes] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._total_published = 0

    @property
    def data(self) -> Deque[bytes]:
        return self._data

    @property
    def info(self) -> SubscriberInfo:
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
        return self._stop_waiter is None or self._stop_waiter.done()

    @property
    def subscriptions(self) -> Set[Subscription]:
        return self._subscriptions

    def stop(self, e: Exception | None) -> None:
        waiter, self._stop_waiter = self._stop_waiter, None
        if waiter is not None and not waiter.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)

    def publish(self, data: str | bytes) -> None:
        assert self._data is not None
        self._data.append(data)
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        self._total_published += 1
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    def start(self, subscriptions: Set[Subscription], maxlen: int) -> asyncio.Future:
        assert self._stop_waiter is None
        assert self._data is None
        
        self._subscriptions.update(subscriptions)
        self._data = deque(maxlen=maxlen)
        
        waiter = self._loop.create_future()
        self._stop_waiter = waiter
        return waiter

    async def wait(self) -> None:
        if self._data_waiter is not None:
            raise RuntimeError("Two coroutines cannot wait for data simultaneously.")
        
        if self.stopped:
            return SubscriberCodes.STOPPED
        
        stop = self._stop_waiter
        waiter = self._loop.create_future()
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

    def __enter__(self) -> "Subscriber":
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


class BaseLock(Lock):
    """Base implementation for a lock."""

    def subscriber_key(self, subscription: Subscription) -> str:
        o = str(hash(subscription)).encode()
        return str(int(hashlib.shake_128(o).hexdigest(16), 16))