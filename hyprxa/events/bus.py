import asyncio
import itertools
import logging
from collections.abc import Sequence
from contextlib import suppress
from contextvars import Context
from datetime import datetime
from typing import Callable, Dict, Set, Tuple

import anyio
from aiormq import Connection
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic

from hyprxa.events.exceptions import EventBusClosed
from hyprxa.events.handler import MongoEventHandler
from hyprxa.events.models import Event, TopicSubscription
from hyprxa.events.subscriber import EventSubscriber
from hyprxa.integrations import SubscriptionLimitError, SubscriptionTimeout
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.events")


class EventBus:
    """Event bus backed by RabbitMQ topic exchange.

    The event bus has 2 primary functions...
        1. Route events to subscribers.
        2. Store events in a database.

    ## Event Routing
    Event routing is backed by RabbitMQ. When the event bus `start` method is
    called, a background service is started which opens a connection to the
    broker and begins receiving published events. The event bus utilizes [topic
    exchanges](https://www.rabbitmq.com/tutorials/tutorial-five-python.html). This
    allows for extremely flexible routing rules to subscribers.
    
    When an event is published to the event bus it has a routing key associated to
    it. When a subscriber subscribes to a sequence of topics a queue is declared
    on the exchange and bound to all topics by a routing key. If an events routing
    key pattern matches the topic subscription routing pattern, the event is routed
    directly to any subscribers in the cluster subscribed to that key.

    If the connection to RabbitMQ drops, the link between the broker and subscriber
    is lost but the subscriber is not dropped (at least not initially). The
    manager will continuously try and re-establish the connection. When the
    connection is re-established the service will pick back up and begin to
    route events to subscribers again. While reconnecting, events can be
    published and buffered on the event bus. These events will be picked up by
    the bus and published on the broker. However, it is not guarenteed that any
    of these buffered events actually make it to the subscriber. The event bus
    will wait for 2 seconds before starting to publish events again. This gives
    some time for subscribers to re-establish their connection to the broker before
    the buffered events are published. If the subscribers do not re-establish their
    connection though, any events published will not reach the subscriber.

    ## Event Storage
    When a event is published to the bus, it is buffered on the storage handler.
    Those events are flushed to the database per the storage handlers settings.
    
    Args:
        factory: A callable that returns an `aiormq.Connection`.
        storage: The event handler to write events to the database.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single bus. If the limit is reached, the bus will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of events that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest events
            will be evicted as new events are added.
        max_buffered_events: The maximum number of events that can be buffered
            on the bus waiting to be published and written to the database.
        subscription_timeout: The time to wait for the broker to be ready
            before rejecting the subscription request.
        reconnect_timeout: The time to wait for the broker to be ready
            before dropping an already connected subscriber.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
        max_failed: The maximum number of failed connection attempts before all
            subscribers and client subscriptions tied to this bus are dropped.
    """
    def __init__(
        self,
        factory: Callable[[], Connection],
        storage: MongoEventHandler,
        exchange: str = "hyprxa.exchange.events",
        max_subscribers: int = 200,
        maxlen: int = 100,
        max_buffered_events: int = 1000,
        subscription_timeout: float = 5,
        reconnect_timeout: float = 60,
        max_backoff: float = 3,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        self._factory = factory
        self._storage = storage
        self._exchange = exchange
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._publish_queue: asyncio.PriorityQueue[Tuple[int, Event]] = asyncio.PriorityQueue(maxsize=max_buffered_events)
        self._storage_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_buffered_events)
        self._subscription_timeout = subscription_timeout
        self._reconnect_timeout = reconnect_timeout
        self._backoff = EqualJitterBackoff(cap=max_backoff, initial=initial_backoff)
        self._max_failed = max_failed

        self._connection: Connection = None
        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Future, EventSubscriber] = {}
        self._publishers: Dict[asyncio.Task, EventSubscriber] = {}
        self._ready: asyncio.Event = asyncio.Event()
        self._runner: asyncio.Task = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._events_published = 0
        self._subscribers_serviced = 0

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    @property
    def subscriptions(self) -> Set[TopicSubscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    def start(self) -> None:
        """Start the event bus."""
        if not self.closed:
            return

        runner: asyncio.Task = Context().run(self._loop.create_task, self._manage_broker_connection())
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

    def close(self) -> None:
        """Close the event bus."""
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()
        self.clear()

    def clear(self) -> None:
        """Clear the publish and storage queues."""
        for queue in (self._publish_queue, self._storage_queue):
            try:
                while True:
                    queue.get_nowait()
                    queue.task_done()
            except asyncio.QueueEmpty:
                pass

    def publish(self, event: Event) -> bool:
        """Publish an event to the bus.
        
        Args:
            event: The event to publish.
        
        Returns:
            bool: If `True` event will be published. If `False` publish queue
                is full, event was not enqueued.
        
        Raises:
            EventBusClosed: The event bus is closed.
        """
        if self.closed:
            raise EventBusClosed()
        if self._publish_queue.full() or self._storage_queue.full():
            return False
        self._publish_queue.put_nowait((1, event))
        self._storage_queue.put_nowait(event)

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        """Subscribe to a sequence of topics on the event bus.
        
        Args:
            subscriptions: The topics to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            EventBusClosed: The event bus is closed.
            EventBusSubscriptionLimitError: The event bus is full.
            EventBusSubscriptionTimeout: Timed out waiting for broker connection.
        """
        if self.closed:
            raise EventBusClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(f"Max subscriptions reached ({self._max_subscribers})")
        
        subscriptions = set(subscriptions)
        connection = await self._wait_for_broker(self._subscription_timeout)
        subscriber = EventSubscriber()
        
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self._subscriber_lost)
        self._subscribers[fut] = subscriber

        self._start_publisher(subscriber, connection)

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        self._subscribers_serviced += 1

        return subscriber

    def _subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        assert fut in self._subscribers
        self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in event subscriber", exc_info=e)

    def _publisher_lost(self, fut: asyncio.Future) -> None:
        """Callback after publishers have stopped."""
        assert fut in self._publishers
        subscriber = self._publishers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in publisher", exc_info=e)
        if not subscriber.stopped:
            fut = self._loop.create_task(self._reconnect_publisher(subscriber))
            fut.add_done_callback(self._background.discard)
            self._background.add(fut)

    def _start_publisher(
        self,
        subscriber: EventSubscriber,
        connection: Connection
    ) -> None:
        """Start a publisher linking a subscriber to the data exchange as a
        background task.
        """
        publisher = self._loop.create_task(
            self._link_subscriber(
                subscriber,
                connection,
                self._exchange
            )
        )
        publisher.add_done_callback(self._publisher_lost)
        self._publishers[publisher] = subscriber

    async def _wait_for_broker(self, timeout: float) -> Connection:
        """Wait for broker connection to be ready."""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for broker to be ready.") from e
        else:
            assert self._connection is not None and not self._connection.is_closed
            return self._connection

    async def _reconnect_publisher(self, subscriber: EventSubscriber) -> None:
        """Wait for broker connection to be reopened then restart publisher."""
        try:
            connection = await self._wait_for_broker(self._reconnect_timeout)
        except SubscriptionTimeout as e:
            subscriber.stop(e)
        else:
            if not subscriber.stopped:
                self._start_publisher(subscriber, connection)

    async def _link_subscriber(
        self,
        subscriber: EventSubscriber,
        connection: Connection,
        exchange: str
    ) -> None:
        """Link a RabbitMQ queue to a subscriber for message routing."""
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            subscriber.publish(data)
        
        channel = await connection.channel(publisher_confirms=False)
        try:
            await channel.exchange_declare(exchange=exchange, exchange_type="topic")
            declare_ok = await channel.queue_declare(exclusive=True)
            
            binds = [
                channel.queue_bind(
                    declare_ok.queue,
                    exchange=exchange,
                    routing_key=f"{subscription.routing_key}"
                )
                for subscription in subscriber.subscriptions
            ]
            await asyncio.gather(*binds)

            await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

            waiter = self._loop.create_task(subscriber.wait_for_stop())
            await asyncio.wait([channel.closing], waiter)
        finally:
            waiter.cancel()
            if not channel.is_closed:
                await channel.close()

    async def _publish_events(self, connection: Connection, exchange: str) -> None:
        """Publish enqueued events to the broker."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="topic")

        # There is no way we can guarentee that buffered events on the bus
        # will be routed to subscribers during a reconnect. Chances are that
        # some events may be published to the exchange and discarded as unroutable
        # because the subscribers have not finished binding to the exchange. The
        # best we can do is wait a couple of seconds before starting to publish.
        # This should allow any subscribers to re-establsh their link to the
        # broker before we publish any buffered events. But again, its not
        # guarenteed that we wont lose anything.
        await asyncio.sleep(2)

        while True:
            _, event = await self._publish_queue.get()
            self._publish_queue.task_done()
            routing_key, payload = event.publish()
            
            confirmation = await channel.basic_publish(
                payload,
                exchange=exchange,
                routing_key=routing_key
            )
            
            match confirmation:
                case Basic.Ack():
                    continue
                case Basic.Nack():
                    fut = self._loop.create_task(self._publish_queue.put((0, event)))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)

    async def _store_messages(self) -> None:
        """Store events in the event database."""
        while True:
            msg = await self._storage_queue.get()
            self._storage_queue.task_done()
            await anyio.to_thread.run_sync(
                self._storage.publish,
                msg.to_document(),
                cancellable=True
            )

    async def _run(self) -> None:
        """Manage background tasks for bus."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._manage_broker_connection)
                tg.start_soon(self._store_messages)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Event bus failed", exc_info=True)
            raise

    async def _manage_broker_connection(self) -> None:
        """Manages broker connection for bus."""
        attempts = 0
        
        while True:
            connection = self._factory()
            _LOGGER.debug("Connecting to %s", connection.url)
            
            try:
                await connection.connect()
            except Exception:
                sleep = self._backoff.compute(attempts)
                _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                await asyncio.sleep(sleep)
                attempts += 1
                
                if attempts >= self._max_failed:
                    _LOGGER.error(
                        "Dropping %i subscribers due to repeated connection failures",
                        len(self._subscribers)
                    )
                    for fut in self._subscribers.keys(): fut.cancel()
                
                continue
            
            else:
                attempts = 0
                self._connection = connection
                self._ready.set()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._publish_events, connection, self._exchange)
                    await asyncio.shield(connection.closing)
            except (Exception, anyio.ExceptionGroup):
                self._ready.clear()
                self._connection = None
                for fut in self._publishers.keys(): fut.cancel()
                with suppress(Exception):
                    await connection.close(timeout=2)
                _LOGGER.warning("Error in event bus", exc_info=True)
            
            sleep = self._backoff.compute(attempts)
            _LOGGER.warning(
                "Event bus unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)