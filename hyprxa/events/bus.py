import asyncio
import logging
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Any, Tuple

import anyio
from aiormq import Channel, Connection
from pamqp import commands

from hyprxa.base import BaseBroker, SubscriptionLimitError
from hyprxa.events.exceptions import EventBusClosed
from hyprxa.events.handler import MongoEventHandler
from hyprxa.events.models import (
    Event,
    EventBusInfo,
    EventDocument,
    TopicSubscription
)
from hyprxa.events.subscriber import EventSubscriber



_LOGGER = logging.getLogger("hyprxa.events.bus")


class EventBus(BaseBroker):
    def __init__(
        self,
        storage: MongoEventHandler,
        *args: Any,
        max_buffered_events: int = 1000,
        **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._storage = storage
        self._publish_queue: asyncio.PriorityQueue[Tuple[int, Event]] = asyncio.PriorityQueue(maxsize=max_buffered_events)
        self._storage_queue: asyncio.Queue[EventDocument] = asyncio.Queue(maxsize=max_buffered_events)

        self._total_published = 0
        self._total_stored = 0

    @property
    def info(self) -> EventBusInfo:
        storage_info = self._storage.worker.info if self._storage.worker else {}
        return EventBusInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            status=self.status,
            created=self.created,
            uptime=(datetime.utcnow() - self.created).total_seconds(),
            active_subscribers=len(self._subscribers),
            active_subscriptions=len(self.subscriptions),
            subscriber_capacity=self.max_subscribers-len(self._subscribers),
            total_subscribers_serviced=self._subscribers_serviced,
            subscriber_info=[subscriber.info for subscriber in self._subscribers.values()],
            publish_buffer_size=self._publish_queue.qsize(),
            storage_buffer_size=self._storage_queue.qsize(),
            total_published_events=self._total_published,
            total_stored_events=self._total_stored,
            storage_info=storage_info
        )

    def close(self) -> None:
        """Close the event bus."""
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
        self._storage_queue.put_nowait(event.to_document())
        return True

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        if self.closed:
            raise EventBusClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(f"Max subscriptions reached ({self._max_subscribers})")
        
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
        """Manage background tasks for bus."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.manage_connection)
                tg.start_soon(self._store_events)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Event bus failed", exc_info=True)
            raise

    async def manage_connection(self) -> None:
        attempts = 0
        connection: Connection = None
        
        try:
            while True:
                connection = self.get_connection()
                _LOGGER.debug("Connecting to %s", connection.url)
                
                try:
                    await connection.connect()
                except Exception:
                    sleep = self.get_backoff(attempts)
                    _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                    await asyncio.sleep(sleep)
                    attempts += 1
                    
                    continue
                
                else:
                    attempts = 0
                    self.set_connection(connection)
                
                try:
                    connection.closing.add_done_callback(lambda _: self.remove_connection())
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._publish_events, connection, self.exchange)
                        await asyncio.shield(connection.closing)
                except (Exception, anyio.ExceptionGroup):
                    with suppress(Exception):
                        await connection.close(timeout=2)
                    _LOGGER.warning("Error in manager", exc_info=True)
                
                sleep = self.get_backoff(0)
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
        """Publish enqueued events to the broker."""
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
            await anyio.to_thread.run_sync(
                self._storage.publish,
                event,
                cancellable=True
            )
            self._total_stored += 1