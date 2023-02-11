import asyncio
import logging
import random
from collections.abc import Sequence
from contextlib import suppress
from contextvars import Context
from datetime import datetime
from typing import Any, Callable, Dict, Set, Type

import anyio
from anyio.abc import TaskStatus
try:
    from aiormq import Connection
    from aiormq.abc import DeliveredMessage
except ImportError:
    pass

from hyprxa.integrations.base import BaseLock
from hyprxa.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionError,
    SubscriptionLimitError,
    SubscriptionLockError,
    SubscriptionTimeout
)
from hyprxa.integrations.models import BrokerStatus, ManagerInfo, Subscription
from hyprxa.integrations.protocols import Client, Subscriber
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.integrations")


class ClientManager:
    """Data integration manager backed by RabbitMQ.

    A manager receives data from clients and routes the data to subscribers.

    Args:
        factory: A callable that returns an `aiormq.Connection`.
        exchange: The exchange name to use.
        lock: The subscription lock.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
        timeout: The time to wait for the backing service to be ready before
            rejecting the subscription request.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
        max_failed: The maximum number of failed connection attempts before all
            subscribers and client subscriptions tied to this manager are dropped.
    """
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        factory: Callable[[],"Connection"],
        lock: BaseLock,
        exchange: str = "hyprxa.exchange.data",
        max_subscribers: int = 500,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 3,
        initial_backoff: float = 1,
        max_failed: int = 15,
    ) -> None:
        self._client = client
        self._subscriber = subscriber
        self._lock = lock
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._timeout = timeout
        self._backoff = EqualJitterBackoff(cap=max_backoff, initial=initial_backoff)
        self._max_failed = max_failed

        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Task, Subscriber] = {}
        self._ready: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._subscribers_serviced = 0

        runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._run(
                factory=factory,
                exchange=exchange
            )
        )
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

    @property
    def closed(self) -> None:
        raise NotImplementedError()

    @property
    def info(self) -> ManagerInfo:
        return ManagerInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            status=self.status,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            active_subscribers=len(self._subscribers),
            active_subscriptions=len(self.subscriptions),
            subscriber_capacity=self._max_subscribers - len(self._subscribers),
            total_subscribers_serviced=self._subscribers_serviced,
            client_info=self._client.info,
            subscriber_info=[subscriber.info for subscriber in self._subscribers.values()]
        )

    @property
    def status(self) -> BrokerStatus:
        if self._ready.is_set():
            return BrokerStatus.CONNECTED
        return BrokerStatus.DISCONNECTED

    @property
    def subscriptions(self) -> Set[Subscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        for fut in self._subscribers.keys(): fut.cancel()
        if not self._client.closed:
            await self._client.close()

    async def subscribe(
        self,
        subscriptions: Sequence[Subscription]
    ) -> Subscriber:
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        
        Returns:
            subscriber: The configured subscriber.
        
        Raises:
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionError: Cannot subscribe.
        """
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)

        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for service to be ready.") from e
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    async def _subscribe(self, subscriptions: Set[Subscription]) -> None:
        try:
            to_subscribe = await self._lock.acquire(subscriptions)
        except Exception as e:
            raise SubscriptionLockError("Unable to acquire locks") from e
        else:
            _LOGGER.debug("Acquired %i locks", len(to_subscribe))

        if to_subscribe:
            try:
                subscribed = await self._client.subscribe(to_subscribe)
            except ClientClosed as e:
                await self._lock.release(to_subscribe)
                await self.close()
                raise ManagerClosed() from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._subscribers
        subscriber = self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Unhandled error in %s", subscriber.__class__.__name__, exc_info=e)

    async def _manage_subscriptions(self) -> None:
        """Background task that manages subscription locking along with client
        and subscriber subscriptions.
        """
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._get_dropped_subscriptions)
            tg.start_soon(self._extend_client_subscriptions)
            tg.start_soon(self._extend_subscriber_subscriptions)
            tg.start_soon(self._poll_client_subscriptions)
            tg.start_soon(self._poll_subscriber_subscriptions)

    async def _get_dropped_subscriptions(self) -> None:
        """Retrieve dropped subscriptions and release client locks."""
        async for msg in self._client.dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                if msg.error:
                    _LOGGER.warning(
                        "Releasing %i locks due to client connection error",
                        len(subscriptions),
                        exc_info=msg.error
                    )
                else:
                    _LOGGER.debug("Releasing %i locks", len(subscriptions))

                await self._lock.release(subscriptions)

    async def _extend_client_subscriptions(self) -> None:
        """Extend client locks owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                await self._lock.extend_client(subscriptions)

    async def _extend_subscriber_subscriptions(self) -> None:
        """Extend subscriber registrations owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                await self._lock.extend_subscriber(subscriptions)

    async def _poll_client_subscriptions(self) -> None:
        """Poll client subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                unsubscribe = await self._lock.client_poll(subscriptions)
                if unsubscribe:
                    _LOGGER.info("Unsubscribing from %i subscriptions", len(unsubscribe))
                    fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
    
    async def _poll_subscriber_subscriptions(self) -> None:
        """Poll subscriber subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                not_subscribed = await self._lock.subscriber_poll(subscriptions)
                if not_subscribed:
                    try:
                        await self._subscribe(not_subscribed)
                    except SubscriptionError:
                        for fut, subscriber in self._subscribers.items():
                            if (
                                not fut.done() and
                                not_subscribed.difference(subscriber.subscriptions) != not_subscribed
                            ):
                                fut.cancel()
                                _LOGGER.warning(
                                    "Subscriber dropped. Unable to pick up lost subscriptions",
                                    exc_info=True
                                )
        
    async def _publish_data(self, connection: "Connection", exchange: str) -> None:
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="direct")

        async for msg in self._client.messages():
            routing_key = str(hash(msg.subscription))
            await channel.basic_publish(
                msg.json().encode(),
                exchange=exchange,
                routing_key=routing_key
            )

    async def _receive_data(
        self,
        connection: "Connection",
        exchange: str,
        task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ) -> None:
        """Retrieve messages from the exchange and publish them to subscribers."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        declare_ok = await channel.queue_declare(exclusive=True)
        await channel.queue_bind(declare_ok.queue, exchange)
        
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(data)
        
        task_status.started()
        await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass