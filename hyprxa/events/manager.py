import asyncio
import logging
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Any, Tuple

import anyio
from aiormq import Channel, Connection
from pamqp import commands

from hyprxa.base.manager import BaseManager
from hyprxa.base.exceptions import SubscriptionLimitError
from hyprxa.events.exceptions import EventManagerClosed
from hyprxa.events.handler import MongoEventHandler
from hyprxa.events.models import EventManagerInfo, EventDocument
from hyprxa.events.subscriber import EventSubscriber
from hyprxa.topics.models import TopicSubscription



_LOGGER = logging.getLogger("hyprxa.events.manager")


class EventManager(BaseManager):
    """Publishes events to RabbitMQ and manages event subscribers.
    
    
    """
    def __init__(
        self,
        storage: MongoEventHandler,
        max_buffered_events: int = 1000,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._storage = storage
        self._publish_queue: asyncio.PriorityQueue[Tuple[int, EventDocument]] = asyncio.PriorityQueue(maxsize=max_buffered_events)
        self._storage_queue: asyncio.Queue[EventDocument] = asyncio.Queue(maxsize=max_buffered_events)

        self._total_published = 0
        self._total_stored = 0

    @property
    def info(self) -> EventManagerInfo:
        return EventManagerInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            status=self.status,
            created=self.created,
            uptime=(datetime.utcnow() - self.created).total_seconds(),
            active_subscribers=len(self.subscribers),
            active_subscriptions=len(self.subscriptions),
            subscriber_capacity=self.max_subscribers-len(self.subscribers),
            total_subscribers_serviced=self._subscribers_serviced,
            subscribers=[subscriber.info for subscriber in self.subscribers.values()],
            publish_buffer_size=self._publish_queue.qsize(),
            storage_buffer_size=self._storage_queue.qsize(),
            total_published=self._total_published,
            total_stored=self._total_stored,
            storage=self._storage.info
        )

    def close(self) -> None:
        """Close the event manager."""
        super().close()
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

    def publish(self, event: EventDocument) -> bool:
        """Publish an event to the manager.
        
        Args:
            event: The event to publish.
        
        Returns:
            bool: If `True` event will be published. If `False` publish queue
                or storage queue is full, event was not enqueued.
        
        Raises:
            EventManagerClosed: The event manager is closed.
        """
        if self.closed:
            raise EventManagerClosed()
        if self._publish_queue.full() or self._storage_queue.full():
            return False
        self._publish_queue.put_nowait((1, event))
        self._storage_queue.put_nowait(event)
        return True

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        """Subscribe to a topic on the manager.
        
        Args:
            subscriptions: The subscription to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            EventManagerClosed: The manager is closed.
            SubscriptionLimitError: The manager is maxed out on subscribers.
            SubscriptionTimeout: Timed out waiting for rabbitmq connection.
        """
        if self.closed:
            raise EventManagerClosed()
        if len(self._subscribers) >= self.max_subscribers:
            raise SubscriptionLimitError(f"Max subscriptions reached ({self.max_subscribers})")
        
        subscriptions = set(subscriptions)

        connection = await self.wait()
        
        subscriber = EventSubscriber()
        self.add_subscriber(subscriber=subscriber, subscriptions=subscriptions)
        self.connect_subscriber(subscriber=subscriber, connection=connection)

        return subscriber

    async def bind_subscriber(
        self,
        subscriber: EventSubscriber,
        channel: Channel,
        declare_ok: commands.Queue.DeclareOk,
    ) -> None:
        """Bind the queue to all subscriber subscriptions."""
        binds = [
            channel.queue_bind(
                declare_ok.queue,
                exchange=self.exchange,
                routing_key=f"{subscription.routing_key}"
            )
            for subscription in subscriber.subscriptions
        ]
        await asyncio.gather(*binds)

    async def run(self) -> None:
        """Manage background tasks for manager."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.manage_connection)
                tg.start_soon(self._store_events)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Event manager failed", exc_info=True)
            raise

    async def manage_connection(self) -> None:
        """Manages RabbitMQ connection for manager."""
        connection: Connection = None
        
        try:
            while True:
                connection = self.get_connection()
                _LOGGER.debug("Connecting to %s", connection.url)
                
                try:
                    await connection.connect()
                except Exception:
                    sleep = self.backoff.compute()
                    _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                    await asyncio.sleep(sleep)
                    
                    continue
                
                else:
                    self.backoff.reset()
                    self.set_connection(connection)
                
                try:
                    # We need to remove the connection as soon as its closed.
                    # In testing, there was a race condition that occurred where
                    # the subscriber reconnect process would kick off before
                    # exiting this context and removing the connection. The
                    # manager state would still be 'ready' but the connection
                    # was closed. So the reconnect coroutine would immediately
                    # assume the connection was ready again and proceed. It would
                    # then check the connection, find it was closed, and fail.
                    # It was all dependant on how the loop scheduled the callbacks.
                    # Removing it as callback to the `closing` future resolves
                    # the issue.
                    connection.closing.add_done_callback(lambda _: self.remove_connection())
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._publish_events, connection, self.exchange)
                        await asyncio.shield(connection.closing)
                except (Exception, anyio.ExceptionGroup):
                    with suppress(Exception):
                        await connection.close(timeout=2)
                    _LOGGER.warning("Error in manager", exc_info=True)
                
                sleep = self.backoff.compute()
                _LOGGER.warning(
                    "Manager unavailable, attempting to reconnect in %0.2f seconds",
                    sleep,
                    exc_info=True
                )
                await asyncio.sleep(sleep)
        finally:
            self.remove_connection()
            if connection is not None and not connection.is_closed:
                with suppress(Exception):
                    await connection.close(timeout=2)

    async def _publish_events(self, connection: Connection, exchange: str) -> None:
        """Publish enqueued events to the manager."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="topic")

        # When the broker connection drops, the link between the broker and
        # subscribers is broken. The subscribers will wait for a new connection
        # and then re-declare all queues and bindings. All queues declared are
        # temporary for obvious reasons so when we have interruptions, the
        # subscribers have to race to re-declare their queues before any events
        # are published otherwise those events will not be routed and will be lost.
        
        # If we only had to worry about subscribers in a single process, we could
        # wait on the declarations before publishing anything. But, when we have
        # multiple processes spanning potentially multiple hosts, there is no
        # way to confirm all subscriber links have been re-established. So
        # instead, all we do is wait a couple of seconds. This should give the
        # subscriber enough time re-establish their link to the broker before
        # anything is published. However, if it takes longer than the waiting
        # period to re-declare the queues and bindings, events will be lost.
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
                case commands.Basic.Ack():
                    self._total_published += 1
                    continue
                case commands.Basic.Nack():
                    # Republish the message as a higher priority.
                    # We need to run this as background task in case the publish
                    # queue is full.
                    self.add_background_task(self._publish_queue.put, (0, event))

    async def _store_events(self) -> None:
        """Store events in the event database."""
        while True:
            event = await self._storage_queue.get()
            self._storage_queue.task_done()
            try:
                await anyio.to_thread.run_sync(
                    self._storage.publish,
                    event,
                    cancellable=True
                )
                self._total_stored += 1
            except TimeoutError:
                _LOGGER.error("Failed to store event. Event will be discarded", exc_info=True)