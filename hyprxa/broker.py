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

    For information on topic exchanges see...
    https://www.rabbitmq.com/tutorials/tutorial-five-python.html
    
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
    """
    def __init__(
        self,
        factory: Callable[[], Connection] | None = None,
        storage: MongoEventHandler | None = None,
        exchange: str = "hyprxa.exchange.events",
        max_subscribers: int = 200,
        maxlen: int = 100,
        max_buffered_events: int = 1000,
        subscription_timeout: float = 5,
        reconnect_timeout: float = 60,
        max_backoff: float = 3,
        initial_backoff: float = 1
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

        self._connection: Connection = None
        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Future, EventSubscriber] = {}
        self._subscriber_links: Dict[asyncio.Task, EventSubscriber] = {}
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

        runner: asyncio.Task = Context().run(self._loop.create_task, self._run())
        runner.add_done_callback(lambda _: self.close())
        self._runner = runner

    def close(self) -> None:
        """Close the event bus."""
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()
        self.clear()
        self._storage.close()

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
                or storage queue is full, event was not enqueued.
        
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

        self._create_link(subscriber, connection)

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

    def _link_lost(self, fut: asyncio.Future) -> None:
        """Callback after link between subscriber and broke is lost due to either
        a subscriber to broker disconnect.
        ."""
        assert fut in self._subscriber_links
        subscriber = self._subscriber_links.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in subscriber link", exc_info=e)
        if not subscriber.stopped:
            # Link lost due to broker disconnect, re-establish link after broker
            # connection is re-established.
            fut = self._loop.create_task(self._reconnect_link(subscriber))
            fut.add_done_callback(self._background.discard)
            self._background.add(fut)

    def _create_link(
        self,
        subscriber: EventSubscriber,
        connection: Connection
    ) -> None:
        """Create a link between a subscriber and the exchange."""
        link = self._loop.create_task(
            self._link_subscriber(
                subscriber,
                connection,
                self._exchange
            )
        )
        link.add_done_callback(self._link_lost)
        self._subscriber_links[link] = subscriber

    async def _wait_for_broker(self, timeout: float) -> Connection:
        """Wait for broker connection to be ready."""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for broker to be ready.") from e
        else:
            assert self._connection is not None and not self._connection.is_closed
            return self._connection

    async def _reconnect_link(self, subscriber: EventSubscriber) -> None:
        """Wait for broker connection to be reopened then restart publisher."""
        try:
            connection = await self._wait_for_broker(self._reconnect_timeout)
        except SubscriptionTimeout as e:
            subscriber.stop(e)
        else:
            # Subscriber may have disconnected while we were waiting for broker
            # connection.
            if not subscriber.stopped:
                self._create_link(subscriber, connection)

    async def _link_subscriber(
        self,
        subscriber: EventSubscriber,
        connection: Connection,
        exchange: str
    ) -> None:
        """Link a subscriber to the exchange.
        
        This binds a queue to each routing key in the subscriber subscriptions.
        """
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

            if subscriber.stopped:
                return
            assert subscriber.waiter is not None and not subscriber.waiter.done()
            await asyncio.wait([channel.closing, subscriber.waiter])
        finally:
            if not channel.is_closed:
                await channel.close()

    async def _run(self) -> None:
        """Manage background tasks for bus."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._manage_broker_connection)
                tg.start_soon(self._store_events)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Event bus failed", exc_info=True)
            raise

    async def _manage_broker_connection(self) -> None:
        """Manages broker connection for bus."""
        attempts = 0
        connection: Connection = None
        try:
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
                    # The existing links are useless, we will be creating
                    # another connection object. Let them begin their reconnect
                    # procedure.
                    for fut in self._subscriber_links.keys(): fut.cancel()
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
        finally:
            self._ready.clear()
            self._connection = None
            if connection is not None and not connection.is_closed:
                with suppress(Exception):
                    await connection.close(timeout=2)