import asyncio
import itertools
import logging
from collections import deque
from collections.abc import Coroutine, Sequence
from contextlib import suppress
from datetime import datetime
from types import TracebackType
from typing import Any, Callable, Deque, Dict, Set, Type


from aiormq import Channel, Connection
from aiormq.abc import DeliveredMessage
from pamqp import commands

from hyprxa.broker.exceptions import SubscriptionTimeout
from hyprxa.broker.models import (
    BrokerInfo,
    BrokerStatus,
    SubscriberCodes,
    SubscriberInfo
)
from hyprxa.models import BaseSubscription
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.broker")


class BaseSubscriber:
    """Base implementation for a subscriber."""
    def __init__(self) -> None:
        self._subscriptions: Set[BaseSubscription] = set()
        self._data: Deque[str] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._total_published = 0

    @property
    def data(self) -> Deque[str]:
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
    def subscriptions(self) -> Set[BaseSubscription]:
        return self._subscriptions

    @property
    def waiter(self) -> asyncio.Future | None:
        return self._stop_waiter

    def stop(self, e: Exception | None) -> None:
        waiter, self._stop_waiter = self._stop_waiter, None
        if waiter is not None and not waiter.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)

    def publish(self, data: bytes) -> None:
        assert self._data is not None
        self._data.append(data.decode())
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        self._total_published += 1
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    def start(self, subscriptions: Set[BaseSubscription], maxlen: int) -> asyncio.Future:
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


class BaseBroker:
    """Data broker backed by a RabbitMQ exchange.
    
    Args:
        factory: A callable that returns an `aiormq.Connection`.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single broker. If the limit is reached, the broker will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of events that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest events
            will be evicted as new events are added.
        subscription_timeout: The time to wait for the broker to be ready
            before rejecting the subscription request.
        reconnect_timeout: The time to wait for the broker to be ready
            before dropping an already connected subscriber.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
    """
    def __init__(
        self,
        factory: Callable[[], Connection],
        exchange: str,
        max_subscribers: int = 200,
        maxlen: int = 100,
        subscription_timeout: float = 5,
        reconnect_timeout: float = 60,
        max_backoff: float = 3,
        initial_backoff: float = 1
    ) -> None:
        self._factory = factory
        self._exchange = exchange
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._subscription_timeout = subscription_timeout
        self._reconnect_timeout = reconnect_timeout
        self._backoff = EqualJitterBackoff(cap=max_backoff, initial=initial_backoff)

        self._connection: Connection = None
        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Future, BaseSubscriber] = {}
        self._subscriber_connections: Dict[asyncio.Task, BaseSubscriber] = {}
        self._ready: asyncio.Event = asyncio.Event()
        self._runner: asyncio.Task = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self.created = datetime.utcnow()
        self._subscribers_serviced = 0

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    @property
    def info(self) -> BrokerInfo:
        raise NotImplementedError()

    @property
    def max_subscribers(self) -> int:
        return self._max_subscribers

    @property
    def status(self) -> BrokerStatus:
        if self._connection is not None and not self._connection.is_closed:
            return BrokerStatus.CONNECTED.value
        return BrokerStatus.DISCONNECTED.value

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    def start(self, *args: Any, **kwargs: Any) -> None:
        """Start the broker."""
        raise NotImplementedError()

    def close(self) -> None:
        """Close the broker."""
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        fut, self.runner = self.runner, None
        if fut is not None:
            fut.cancel()

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> BaseSubscriber:
        """Subscribe to a sequence of topics on the event bus.
        
        Args:
            subscriptions: The topics to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            BrokerClosed: The broker is closed.
            SubscriptionLimitError: The broker is maxed out on subscribers.
            SubscriptionTimeout: Timed out waiting for rabbitmq connection.
        """
        raise NotImplementedError()

    async def bind_subscriber(
        self,
        subscriber: BaseSubscriber,
        channel: Channel,
        declare_ok: commands.Queue.DeclareOk,
    ) -> None:
        """Bind the queue to all subscriber subscriptions."""
        raise NotImplementedError()

    async def run(self) -> None:
        """Manage background tasks for broker."""
        raise NotImplementedError()

    async def manage_connection(self) -> None:
        """Manages RabbitMQ connection for broker."""
        raise NotImplementedError()

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        assert fut in self._subscribers
        self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in event subscriber", exc_info=e)

    def subscriber_disconnected(self, fut: asyncio.Future) -> None:
        """Callback after connection between subscriber and broke is lost due to
        either a subscriber or broker disconnect.
        ."""
        assert fut in self._subscriber_connections
        subscriber = self._subscriber_connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in subscriber connection", exc_info=e)
        if not subscriber.stopped:
            # Connection lost due to broker disconnect, re-establish after broker
            # connection is re-established.
            self.add_background_task(self._reconnect_subscriber)

    def add_subscriber(
        self,
        subscriber: BaseSubscriber,
        subscriptions: Set[BaseSubscription]
    ) -> None:
        fut = subscriber.start(subscriptions=subscriptions, maxlen=self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber
        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        self._subscribers_serviced += 1

    def add_background_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *args: Any,
        **kwargs: Any
    ) -> None:
        fut = self._loop.create_task(coro(*args, **kwargs))
        fut.add_done_callback(self._background.discard)
        self._background.add(fut)

    def connect_subscriber(
        self,
        subscriber: BaseSubscriber,
        connection: Connection
    ) -> None:
        """Create a connection between a subscriber and the exchange."""
        subscriber_connection = self._loop.create_task(
            self._connect_subscriber(
                subscriber,
                connection,
                self._exchange
            )
        )
        subscriber_connection.add_done_callback(self.subscriber_disconnected)
        self._subscriber_connections[subscriber_connection] = subscriber

    def get_connection(self) -> Connection:
        return self._factory()

    def remove_connection(self) -> None:
        self._ready.clear()
        self._connection = None

    def set_connection(self, connection: Connection) -> None:
        self._connection = connection
        self._ready.set()

    def get_backoff(self, attempts: int) -> float:
        return self._backoff.compute(attempts)

    async def wait(self, timeout: float | None = None) -> Connection:
        """Wait for broker connection to be ready."""
        timeout = timeout or self._subscription_timeout
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for broker to be ready.") from e
        else:
            assert self._connection is not None and not self._connection.is_closed
            return self._connection

    async def _reconnect_subscriber(self, subscriber: BaseSubscriber) -> None:
        """Wait for RabbitMQ connection to be re-opened then restart subscriber
        connection."""
        try:
            connection = await self.wait(self._reconnect_timeout)
        except SubscriptionTimeout as e:
            subscriber.stop(e)
        else:
            # Subscriber may have disconnected while we were waiting for broker
            # connection.
            if not subscriber.stopped:
                self.connect_subscriber(subscriber, connection)

    async def _connect_subscriber(
        self,
        subscriber: BaseSubscriber,
        connection: Connection,
        exchange: str
    ) -> None:
        """Connect a subscriber to the exchange.
        
        This binds a queue to each routing key in the subscriber subscriptions and
        then watches the channel and subscriber to stop.
        """
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            subscriber.publish(data)
        
        channel = await connection.channel(publisher_confirms=False)
        try:
            await channel.exchange_declare(exchange=exchange, exchange_type="topic")
            declare_ok = await channel.queue_declare(exclusive=True)
            
            await self.bind_subscriber(
                subscriber=subscriber,
                channel=channel,
                declare_ok=declare_ok
            )

            await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

            if subscriber.stopped:
                return
            assert subscriber.waiter is not None and not subscriber.waiter.done()
            
            await asyncio.wait([channel.closing, subscriber.waiter])
        finally:
            if not channel.is_closed:
                await channel.close()