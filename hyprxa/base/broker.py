import asyncio
import itertools
import logging
from collections.abc import Coroutine, Sequence
from contextlib import suppress
from contextvars import Context
from datetime import datetime
from typing import Any, Callable, Dict, List, Set


from aiormq import Channel, Connection
from aiormq.abc import DeliveredMessage
from pamqp import commands

from hyprxa.base.exceptions import SubscriptionTimeout
from hyprxa.base.models import BaseSubscription, BrokerInfo, BrokerStatus
from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.base")


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
        """`True` if the broker is closed."""
        return self._runner is None or self._runner.done()

    @property
    def info(self) -> BrokerInfo:
        """Return current information on the broker."""
        raise NotImplementedError()
    
    @property
    def exchange(self) -> str:
        """Returns the exchange name the broker declared on the RabbitMQ server."""
        return self._exchange

    @property
    def max_subscribers(self) -> int:
        """Returns the maximum number of subscribers this manager can support."""
        return self._max_subscribers

    @property
    def status(self) -> BrokerStatus:
        """Return the status of the RabbitMQ connection."""
        if self._connection is not None and not self._connection.is_closed:
            return BrokerStatus.CONNECTED.value
        return BrokerStatus.DISCONNECTED.value

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all subscribers."""
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    def start(self) -> None:
        """Start the broker."""
        if not self.closed:
            return

        runner: asyncio.Task = Context().run(self._loop.create_task, self.run())
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

    def close(self) -> None:
        """Close the broker."""
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> BaseSubscriber:
        """Subscribe to a sequence of subscriptions on the broker.
        
        Args:
            subscriptions: The subscriptions to subscriber to.
        
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
        """Bind the queue to all subscriber subscriptions.
        
        This method derives the routing keys from the subscriber's subscriptions.
        """
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
        """Add a subscriber to the broker."""
        fut = subscriber.start(subscriptions=subscriptions, maxlen=self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber
        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        self._subscribers_serviced += 1

    def add_background_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *args: Any,
        callbacks: List[Callable[[asyncio.Future], None]],
        **kwargs: Any
    ) -> None:
        """Add a background task to the broker."""
        fut = self._loop.create_task(coro(*args, **kwargs))
        for callback in callbacks:
            fut.add_done_callback(callback)
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
        """Get a new connection object."""
        return self._factory()

    def remove_connection(self) -> None:
        """Remove the connection from the broker. New subscribers will not
        use the existing connection. Drops all subscriber connections.
        """
        self._ready.clear()
        self._connection = None
        for fut in self._subscriber_connections.keys(): fut.cancel()

    def set_connection(self, connection: Connection) -> None:
        """Set the connection for the broker. New subscribers will use this
        connection.
        """
        self._connection = connection
        self._ready.set()
        _LOGGER.debug("Connection established")

    def get_backoff(self, attempts: int) -> float:
        """Get the backoff timeout based on the number of connection attempts
        to RabbitMQ.
        """
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
        connection.
        """
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
            assert subscriber.stopping is not None and not subscriber.stopping.done()
            
            await asyncio.wait([channel.closing, subscriber.stopping])
        finally:
            if not channel.is_closed:
                await channel.close()

    def __del__(self):
        try:
            if not self.closed:
                self.close()
        except Exception:
            pass