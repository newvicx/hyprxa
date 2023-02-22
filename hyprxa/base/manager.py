import asyncio
import atexit
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
from hyprxa.base.models import BaseSubscription, ManagerInfo, ManagerStatus
from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.base.manager")


class BaseManager:
    """Data manager backed by a RabbitMQ exchange.

    `BaseManager` implements the common plumbing for both the `EventManager` and
    `TimeseriesManager`.
    
    Args:
        factory: A callable that returns an `aiormq.Connection`.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of events that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest events
            will be evicted as new events are added.
        subscription_timeout: The time to wait for the manager to be ready
            before rejecting the subscription request.
        reconnect_timeout: The time to wait for the manager to be ready
            before dropping an already connected subscriber.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the manager.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the manager.
    """
    def __init__(
        self,
        factory: Callable[[], Connection],
        exchange: str,
        max_subscribers: int = 100,
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

        self.created = datetime.utcnow()
        self._subscribers_serviced = 0

        atexit.register(self.close)

    @property
    def backoff(self) -> EqualJitterBackoff:
        """Returns the backoff instance for the manager."""
        return self._backoff

    @property
    def closed(self) -> bool:
        """`True` if the manager is closed."""
        return self._runner is None or self._runner.done()

    @property
    def info(self) -> ManagerInfo:
        """Return current information on the manager."""
        raise NotImplementedError()
    
    @property
    def exchange(self) -> str:
        """Returns the exchange name the manager declared on the RabbitMQ server."""
        return self._exchange

    @property
    def exchange_type(self) -> str:
        """Returns the exchange type used by the manager."""
        raise NotImplementedError()

    @property
    def max_subscribers(self) -> int:
        """Returns the maximum number of subscribers this manager can support."""
        return self._max_subscribers

    @property
    def status(self) -> ManagerStatus:
        """Return the status of the RabbitMQ connection."""
        if self._connection is not None and not self._connection.is_closed:
            return ManagerStatus.CONNECTED.value
        return ManagerStatus.DISCONNECTED.value

    @property
    def subscribers(self) -> Dict[asyncio.Future, BaseSubscriber]:
        """Returns all manager subscribers."""
        return self._subscribers

    @property
    def subscribers_serviced(self) -> int:
        """Returns the number of subscribers serviced by this manager."""
        return self._subscribers_serviced

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all subscribers."""
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    def close(self) -> None:
        """Close the manager."""
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()

    async def start(self) -> None:
        """Start the manager."""
        if not self.closed:
            return

        runner: asyncio.Task = Context().run(asyncio.create_task, self.run())
        runner.add_done_callback(lambda _: self.close())
        self._runner = runner

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> BaseSubscriber:
        """Subscribe to a sequence of subscriptions on the manager.
        
        Args:
            subscriptions: The subscriptions to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            ManagerClosed: The manager is closed.
            SubscriptionLimitError: The manager is maxed out on subscribers.
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
        """Manage background tasks for manager."""
        raise NotImplementedError()

    async def manage_connection(self) -> None:
        """Manages RabbitMQ connection for manager."""
        raise NotImplementedError()

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        _LOGGER.debug("Subscriber lost")
        assert fut in self._subscribers
        self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in event subscriber", exc_info=e)

    def subscriber_disconnected(self, fut: asyncio.Future) -> None:
        """Callback after connection between subscriber and broke is lost due to
        either a subscriber or manager disconnect.
        ."""
        _LOGGER.debug("Subscriber disconnect")
        assert fut in self._subscriber_connections
        subscriber = self._subscriber_connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in subscriber connection", exc_info=e)
        if not subscriber.stopped:
            # Connection lost due to RabbitMQ disconnect, re-establish subscriber
            # connection after manager connection is re-established.
            _LOGGER.debug("Attempting to reconnect subscriber")
            self.add_background_task(self._reconnect_subscriber, subscriber)

    def add_subscriber(
        self,
        subscriber: BaseSubscriber,
        subscriptions: Set[BaseSubscription]
    ) -> None:
        """Add a subscriber to the manager."""
        fut = subscriber.start(subscriptions=subscriptions, maxlen=self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber
        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        self._subscribers_serviced += 1

    def add_background_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *args: Any,
        callbacks: List[Callable[[asyncio.Future], None]] = [],
        **kwargs: Any
    ) -> None:
        """Add a background task to the manager."""
        fut = asyncio.create_task(coro(*args, **kwargs))
        for callback in callbacks:
            fut.add_done_callback(callback)
        fut.add_done_callback(self._log_background_result)
        fut.add_done_callback(self._background.discard)
        self._background.add(fut)
        _LOGGER.debug("Started task %s (%s)", fut.get_name(), coro.__qualname__)

    def _log_background_result(self, fut: asyncio.Task) -> None:
        """Log the result of a background task on the manager."""
        _LOGGER.debug("Task %s completed", fut.get_name())
        try:
            e = fut.exception()
        except asyncio.CancelledError:
            _LOGGER.debug("Task %s was cancelled", fut.get_name())
        else:
            if e is not None:
                _LOGGER.warning("Error in task %s", fut.get_name(), exc_info=e)

    def connect_subscriber(
        self,
        subscriber: BaseSubscriber,
        connection: Connection
    ) -> None:
        """Create a connection between a subscriber and the exchange."""
        subscriber_connection = asyncio.create_task(
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
        """Remove the connection from the manager. New subscribers will not
        use the existing connection. Drops all subscriber connections.
        """
        self._ready.clear()
        self._connection = None
        for fut in self._subscriber_connections.keys(): fut.cancel()

    def set_connection(self, connection: Connection) -> None:
        """Set the connection for the manager. New subscribers will use this
        connection.
        """
        self._connection = connection
        self._ready.set()
        _LOGGER.debug("Connection established")

    async def wait(self, timeout: float | None = None) -> Connection:
        """Wait for RabbitMQ connection to be ready."""
        timeout = timeout or self._subscription_timeout
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for connection to be ready.") from e
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
            _LOGGER.warning("Failed to reconnect subscriber", exc_info=True)
            subscriber.stop(e)
        else:
            # Subscriber may have disconnected while we were waiting for manager
            # connection.
            if not subscriber.stopped:
                self.connect_subscriber(subscriber, connection)
                _LOGGER.info("Subscriber reconnected")

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
            await channel.exchange_declare(exchange=exchange, exchange_type=self.exchange_type)
            declare_ok = await channel.queue_declare(exclusive=True, auto_delete=True)
            
            await self.bind_subscriber(
                subscriber=subscriber,
                channel=channel,
                declare_ok=declare_ok
            )

            consume_ok = await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

            if subscriber.stopped:
                return
            assert subscriber.stopping is not None and not subscriber.stopping.done()
            
            await asyncio.wait([channel.closing, subscriber.stopping], return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not channel.closing.done():
                # Subscriber stopped
                await channel.basic_cancel(consume_ok.consumer_tag)
                await channel.close()

    def __del__(self):
        try:
            if not self.closed:
                self.close()
        except Exception:
            pass