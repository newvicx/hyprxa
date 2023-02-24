import asyncio
import logging
import random
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Any, Set

import anyio
from aiormq import Channel, Connection
from fastapi import HTTPException, status
from pamqp import commands
from pydantic import ValidationError

from hyprxa.base.manager import BaseManager
from hyprxa.base.exceptions import SubscriptionLimitError
from hyprxa.timeseries.base import BaseIntegration
from hyprxa.timeseries.exceptions import (
    IntegrationClosed,
    IntegrationSubscriptionError,
    SubscriptionError,
    SubscriptionLockError,
    TimeseriesManagerClosed
)
from hyprxa.timeseries.handler import MongoTimeseriesHandler
from hyprxa.timeseries.lock import SubscriptionLock
from hyprxa.timeseries.models import (
    BaseSourceSubscription,
    SubscriptionMessage,
    TimeseriesManagerInfo
)
from hyprxa.timeseries.sources import Source
from hyprxa.timeseries.subscriber import TimeseriesSubscriber
from hyprxa.util.asyncutils import add_event_loop_shutdown_callback



_LOGGER = logging.getLogger("hyprxa.timeseries.manager")


class TimeseriesManager(BaseManager):
    """Manages subscribers and an integration connecting to a data source.

    The timeseries manager uses RabbitMQ [direct](https://www.rabbitmq.com/tutorials/tutorial-four-python.html)
    exchanges to route `SubscriptionMessages` to subscribers. The routing key
    is a combination of the subscription source and the hash of the subscription.

    The `TimeseriesManager` works cooperatively in a distributed context. It
    maintains a global lock of all integration subscriptions through a caching server
    (Memcached). This ensures that two managers in two different processes do
    not subscribe to the same subscription on their integrations. This allows hyprxa
    to scale horizontally without increasing load on the data source. However,
    if there is a transient error in the caching server, there is a chance that
    two managers may subscribe to the same subscription when the connection is
    restored. In that case, duplicate messages will be posted to RabbitMQ and
    routed to the subscribers. Therefore, it is the responsibility of the subscriber
    to ensure it does not forward along duplicate messages.

    If the connection to RabbitMQ is lost while subscribers are streaming messages,
    the manager will handle reconnecting in the background without disconnecting
    the subscribers. Once the connection is re-established, the manager will
    wait 2 seconds and then begin sending buffered messages. This should provide
    enough time for the manager to complete re-binding subscribers to the
    exchange before any messages are sent. If a message is sent but the subscriber
    has not been bounded to the exchange, the subscriber will not see that event.

    If the RabbitMQ connection is down for an extended period of time, the manager
    will eventually unsubscribe from all subscriptions on the integration and drop
    any remaining subscribers. This happens after `max_failed` reconnect attempts.
    However, the manager will continue to try and reconnect to RabbitMQ in the
    background. Once it reconnects, it can begin accepting new subscribers.

    Every message published to the manager is also persisted to MongoDB. The
    `MongoTimeseriesHandler` manages a background thread which writes messages to
    the collection. If the handler is unable to connect to the database, those
    messages will be lost. The manager does not re-buffer messages that it cannot
    write to the database.

    If an integration error occurs and subscriptions are dropped. The manager will
    drop all subscribers it owns which rely on the dropped subscriptions. Other
    managers in other processes may attempt to subscribe to the dropped
    subscriptions before dropping their subscribers.

    Args:
        source: The data source to connect to.
        lock: The lock instance connected to Memcached.
        storage: The `MongoTimeseriesHandler` for storing timeseries samples.
        max_buffered_message: The maximum number of messages that can be buffered
            on the manager for the storage handler to process. The manager will
            stop pulling messages from the integration until the storage buffer is
            drained.
        max_failed: The maximum number of reconnect attempts to RabbitMQ before
            dropping all integration subscriptions and subscribers.
    """
    def __init__(
        self,
        source: Source,
        lock: SubscriptionLock,
        storage: MongoTimeseriesHandler,
        max_buffered_messages: int = 1000,
        max_failed: int = 15,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._source = source
        self._lock = lock
        self._storage = storage
        self._storage_queue: asyncio.Queue[SubscriptionMessage] = asyncio.Queue(maxsize=max_buffered_messages)
        self._max_failed = max_failed

        self._integration: BaseIntegration = None

        self._total_published = 0
        self._total_stored = 0

    @property
    def exchange_type(self) -> str:
        return "direct"

    @property
    def info(self) -> TimeseriesManagerInfo:
        """Return statistics on the manager instance. Useful for monitoring and
        debugging.
        """
        return TimeseriesManagerInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            status=self.status,
            created=self.created,
            uptime=(datetime.utcnow() - self.created).total_seconds(),
            active_subscribers=len(self.subscribers),
            active_subscriptions=len(self.subscriptions),
            subscriber_capacity=self.max_subscribers-len(self.subscribers),
            total_subscribers_serviced=self.subscribers_serviced,
            subscribers=[subscriber.info for subscriber in self.subscribers.values()],
            source=self._source.source,
            integration=self._integration.info,
            lock=self._lock.info,
            total_published=self._total_published,
            total_stored=self._total_stored,
            storage=self._storage.info,
            storage_buffer_size=self._storage_queue.qsize()
        )
        
    def close(self) -> None:
        """Close the manager."""
        super().close()
        if self._integration is not None:
            self._integration.clear()
        self.clear()
        self._storage.close()
    
    def clear(self) -> None:
        """Clear the storage queue."""
        try:
            while True:
                self._storage_queue.get_nowait()
                self._storage_queue.task_done()
        except asyncio.QueueEmpty:
            pass

    async def start(self) -> None:
        """Start the manager."""
        self._integration = self._source()
        # If the event loop shutsdown, `run` may not be able to close the integration
        # so we add it as shutdown callback.
        await add_event_loop_shutdown_callback(self._integration.close)
        await super().start()

    async def subscribe(self, subscriptions: Sequence[BaseSourceSubscription]) -> TimeseriesSubscriber:
        """Subscribe to a sequence of subscriptions on the manager.
        
        Args:
            subscriptions: The subscriptions to subscriber to.
        
        Returns:
            subscriber: The subscriber instance.

        Raises:
            TimeseriesManagerClosed: The manager is closed.
            SubscriptionLimitError: The manager is maxed out on subscribers.
            SubscriptionTimeout: Timed out waiting for rabbitmq connection.
        """
        if self.closed:
            raise TimeseriesManagerClosed()
        if len(self.subscribers) >= self.max_subscribers:
            raise SubscriptionLimitError(f"Max subscriptions reached ({self.max_subscribers})")

        connection = await self.wait()
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = TimeseriesSubscriber()
        self.add_subscriber(subscriber=subscriber, subscriptions=subscriptions)
        self.connect_subscriber(subscriber=subscriber, connection=connection)

        return subscriber

    async def _subscribe(self, subscriptions: Set[BaseSourceSubscription]) -> None:
        """Acquire locks for subscriptions and subscribe on the integration."""
        try:
            to_subscribe = await self._lock.acquire(subscriptions)
        except Exception as e:
            raise SubscriptionLockError("Unable to acquire locks") from e
        else:
            _LOGGER.debug("Acquired %i locks", len(to_subscribe))

        if to_subscribe:
            try:
                subscribed = await self._integration.subscribe(to_subscribe)
            except IntegrationClosed as e:
                await self._lock.release(to_subscribe)
                await self.close()
                raise TimeseriesManagerClosed() from e
            except Exception as e:
                _LOGGER.warning("Error subscribing on integration", exc_info=True)
                await self._lock.release(to_subscribe)
                raise IntegrationSubscriptionError("An error occurred while subscribing.") from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise IntegrationSubscriptionError("Integration refused subscriptions.")
    
    async def bind_subscriber(
        self,
        subscriber: TimeseriesSubscriber,
        channel: Channel,
        declare_ok: commands.Queue.DeclareOk,
    ) -> None:
        """Bind the queue to all subscriber subscriptions."""
        source = self._source.source
        binds = [
            channel.queue_bind(
                declare_ok.queue,
                exchange=self.exchange,
                routing_key=f"{source}-{hash(subscription)}"
            )
            for subscription in subscriber.subscriptions
        ]
        await asyncio.gather(*binds)
            
    async def run(self) -> None:
        """Manage background tasks for manager."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.manage_connection)
                tg.start_soon(self._store_messages)
                tg.start_soon(self._manage_subscriptions)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Manager failed", exc_info=True)
            raise
        finally:
            await self._integration.close()

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
                    
                    if self.backoff.failures >= self._max_failed:
                        _LOGGER.error(
                            "Dropping %i integration subscriptions due to repeated "
                            "connection failures",
                            len(self._integration.subscriptions)
                        )
                        callbacks = [lambda _: self._integration.clear]
                        self.add_background_task(
                            self._integration.unsubscribe,
                            self._integration.subscriptions,
                            callbacks=callbacks
                        )
                        # Drop any remaining subscribers if the reconnect timeout
                        # is very long.
                        for fut in self.subscribers.keys(): fut.cancel()
                    
                    continue
                
                else:
                    self.backoff.reset()
                    self.set_connection(connection)
                
                try:
                    # Remove the connection from the manager immediately after
                    # the RabbitMQ connection closes. this ensures the manager
                    # state is "not ready" when subscribers try and reconnect.
                    connection.closing.add_done_callback(lambda _: self.remove_connection())
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._publish_messages, connection, self.exchange)
                        await asyncio.shield(connection.closing)
                except IntegrationClosed:
                    with suppress(Exception):
                        await connection.close(timeout=2)
                    _LOGGER.info("Exited manager because integration is closed")
                    raise
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

    async def _publish_messages(self, connection: Connection, exchange: str) -> None:
        """Retrieve messages from the integration and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type=self.exchange_type)

        # After reconnecting to RabbitMQ, we cant reliably confirm all
        # subscribers have binded to the exchange before publishing buffered
        # messages, especially on multiple hosts. We give the subscribers 2 seconds
        # after the connection is made then begin publishing. If a subscriber
        # has not re-declared in that time, the subscriber will not receive
        # those messages.
        await asyncio.sleep(2)

        source = self._source.source
        async for msg in self._integration.get_messages():
            if not isinstance(msg, SubscriptionMessage):
                _LOGGER.warning("Received invalid message type %s", type(msg))
                continue
            await self._storage_queue.put(msg)
            routing_key = f"{source}-{hash(msg.subscription)}"
            await channel.basic_publish(
                msg.json().encode(),
                exchange=exchange,
                routing_key=routing_key
            )
            self._total_published += 1
    
    async def _store_messages(self) -> None:
        """Store messages in the timeseries database."""
        source = self._source.source
        while True:
            msg = await self._storage_queue.get()
            self._storage_queue.task_done()
            try:
                await anyio.to_thread.run_sync(
                    self._storage.publish,
                    msg.to_samples(source),
                    cancellable=True
                )
                self._total_stored += 1
            except TimeoutError:
                _LOGGER.error("Failed to store message. Message will be discarded")

    async def _manage_subscriptions(self) -> None:
        """Background task that manages subscription locking along with integration
        and subscriber subscriptions.
        """
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._get_dropped_subscriptions)
            tg.start_soon(self._extend_integration_subscriptions)
            tg.start_soon(self._extend_subscriber_subscriptions)
            tg.start_soon(self._poll_integration_subscriptions)
            tg.start_soon(self._poll_subscriber_subscriptions)

    async def _get_dropped_subscriptions(self) -> None:
        """Retrieve dropped subscriptions and release integration locks."""
        async for msg in self._integration.get_dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                if msg.error:
                    _LOGGER.warning(
                        "Releasing %i locks due to integration connection error",
                        len(subscriptions),
                        exc_info=msg.error
                    )
                else:
                    _LOGGER.debug("Releasing %i locks", len(subscriptions))

                # If we fail to release the locks, there is at most a TTL gap
                # between the time `release` fails to the actual release due to
                # expiration on the server. In that time, another subscriber in
                # a different process can subscribe to the unreleased subscription.
                # The manager for that process will try to acquire the locks and
                # see they are already held and assume the data is being
                # streamed elsewhere. In actuality that data is not being
                # streamed. Therefore we have a window where a subscriber
                # expects to receive data it will never get. This is, at most,
                # 2.5 TTL. When the manager in the other process polls its
                # subscribers against the caching server, it will see that the
                # integration locks are no longer held and attempt to subscribe
                # on its own integration.
                await self._lock.release(subscriptions)

    async def _extend_integration_subscriptions(self) -> None:
        """Extend integration locks owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._integration.subscriptions
            if subscriptions:
                await self._lock.extend_integration(subscriptions)

    async def _extend_subscriber_subscriptions(self) -> None:
        """Extend subscriber registrations owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                await self._lock.extend_subscriber(subscriptions)

    async def _poll_integration_subscriptions(self) -> None:
        """Poll integration subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._integration.subscriptions
            if subscriptions:
                unsubscribe = await self._lock.integration_poll(subscriptions)
                if unsubscribe:
                    _LOGGER.info("Unsubscribing from %i subscriptions", len(unsubscribe))
                    self.add_background_task(
                        self._integration.unsubscribe,
                        unsubscribe
                    )
    
    async def _poll_subscriber_subscriptions(self) -> None:
        """Poll subscriber subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                not_subscribed = await self._lock.subscriber_poll(subscriptions)
                if not_subscribed:
                    # If the Memcached server goes down, the owner of a integration
                    # subscription will continue to stream data (on failure it
                    # assumes the subscriptions are still needed). When Memcached
                    # comes back, a manager in a different process may subscribe
                    # to subscriptions it thinks are missing before the owning
                    # prcess gets a chance to re-extend the locks for them.
                    # As a result, we would end up streaming duplicate subscriptions
                    # in two different processes.
                    try:
                        await self._subscribe(not_subscribed)
                    except SubscriptionError:
                        for fut, subscriber in self.subscribers.items():
                            if not_subscribed.difference(subscriber.subscriptions) != not_subscribed:
                                fut.cancel()
                                _LOGGER.warning(
                                    "Subscriber dropped. Unable to pick up lost subscriptions",
                                    exc_info=True
                                )