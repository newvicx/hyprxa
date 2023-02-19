import asyncio
import logging
import random
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Any, Set, Type

import anyio
from aiormq import Channel, Connection
from pamqp import commands

from hyprxa.base.manager import BaseManager
from hyprxa.base.exceptions import SubscriptionLimitError
from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.timeseries.base import BaseClient
from hyprxa.timeseries.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
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
from hyprxa.util.asyncutils import add_event_loop_shutdown_callback



_LOGGER = logging.getLogger("hyprxa.timeseries.manager")


class TimeseriesManager(BaseManager):
    """Manages subscribers and a client connection to a data source.
    
    A `TimeseriesManager` receives standard formatted messages from clients
    and broadcasts them out to subscribers via a routing key. The routing key
    is a unique combination of the source name and the hash of the subscription.

    Subscribers subscribe to a sequence of subscriptions and declare a queue on
    RabbitMQ exchange. The queue is bound to all subscriptions via an analogous
    routing key (source-hash) that the manager uses when publishing. Messages
    are routed directly to subscribers from the RabbitMQ server.

    The `TimeseriesManager` works cooperatively in a distributed context. It
    maintains a global lock of all client subscriptions through a caching server
    (Memcached). This ensures that two managers in two different processes do
    not subscribe to the same subscription on their clients. This allows hyprxa
    to scale horizontally without increasing load on the data source. However,
    if there is a transient error in the caching server, there is a chance that
    two managers may subscribe to the same subscription when the connection is
    restored. In that case, duplicate messages will be posted to RabbitMQ and
    routed to the subscribers. Therefore, it is the responsibility of the subscriber
    to ensure it does not forward along duplicate messages. This can be done
    easily assuming the client adheres to the hyprxa protocol...
    
        - Clients must only send messages containing unique timestamped values in
        monotonically increasing order. `TimestampedValue`(s) must be sorted
        within a message.

    For a client, it is trivial to sort timestamped values (`TimestampValue` is
    sortable) and guarentee uniqueness of timestamps. From there a client need
    only store a reference to the last timestamp for that subscription.
    On the next message it sorts and then filters to meet the uniqueness and
    monotonically increasing requirements. Some sources may already meet this
    requirement in which case the client may not need to do anything but it is
    the responsibility of the client to ensure these requirements are met.

    If a client is compliant, a subscriber need only keep a reference to the
    last (i.e. most recent) timestamp for each subscription. If it receives
    a message where the last timestamp is less than or equal to the last timestamp
    of the previous message it discards the message.

    If the connection to RabbitMQ is lost for any reason, subscribers will
    not be dropped (at least not initially). The manager will attempt to re-establish
    the connection while subscribers wait (up to `reconnect_seconds`). While the
    manager is reconnecting to RabbitMQ, clients continue to operate as usual
    and will buffer messages on the manager. When the connection is re-established,
    subscribers will declare new queues and bind to the exchange. The manager
    will pick up buffered messages and publish them. From the perspective of
    the client, nothing happened. However, there is no guarentee that messages
    buffered on the manager while reconnecting will reach the subscriber. This
    is because there is no way to confirm all subscribers have re-binded to the
    exchange in every process on every host. The manager will wait 2 seconds
    before it publishes the first buffered message. If a subscriber has not
    completed binding to the exchange, that message and all subsequent messages
    will be lost until the subscriber binds to the exchange. In practice, 2
    seconds is a long time to re-declare a queue and bind so while it is unlikely
    that any messages will be lost we cannot guarentee they wont.

    If the RabbitMQ connection is down for an extended period of time, the manager
    will eventually unsubscribe from all subscriptions on the client and drop
    any remaining subscribers. This happens after `max_failed` reconnect attempts.
    However, the manager will continue to try and reconnect to RabbitMQ in the
    background. Once it reconnects, it can begin accepting new subscribers.

    Another interesting situation arises when the RabbitMQ connection is down
    for an extended period. The client continues to operate in the background
    and buffer messages from its connections. The data queue on a client is
    bounded and can eventually fill up. At that point, connections will block
    when trying to publish to the client. This ensures that messages do not pile
    up indefinetely consuming more and more memory. This flow control also kicks
    in if the manager is unable to connect to MongoDB for storing samples.

    When a manager receives a message, it will store all timeseries samples within
    that message in the timeseries database. For more information on storage
    worker semantics, see the `MongoTimeseriesHandler`.

    Args:
        source: The data source to connect to.
        lock: The lock instance connected to Memcached.
        storage: The `MongoTimeseriesHandler` for storing timeseries samples.
        max_buffered_message: The maximum number of messages that can be buffered
            on the manager for the storage handler to process. The manager will
            stop pulling messages from the client until the storage buffer is
            drained.
        max_failed: The maximum number of reconnect attempts to RabbitMQ before
            dropping all client subscriptions and subscribers.
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

        self._client: BaseClient = None
        self._subscriber: Type[BaseSubscriber] = None

        self._total_published = 0
        self._total_stored = 0

    @property
    def info(self) -> TimeseriesManagerInfo:
        """Return statistics on the manager instance. Useful for monitoring and
        debugging.
        """
        storage_info = self._storage.worker.info if self._storage.worker else None
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
            client=self._client.info,
            lock=self._lock.info,
            total_published=self._total_published,
            total_stored=self._total_stored,
            storage=storage_info,
            storage_buffer_size=self._storage_queue.qsize()
        )
        
    def close(self) -> None:
        """Close the manager."""
        super().close()
        if self._client is not None:
            self._client.clear()
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
        self._client, self._subscriber = self._source()
        # If the event loop shutsdown, `run` may not be able to close the client
        # so we add it as shutdown callback.
        await add_event_loop_shutdown_callback(self._client.close)
        await super().start()

    async def subscribe(self, subscriptions: Sequence[BaseSourceSubscription]) -> BaseSubscriber:
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
        
        subscriptions = set(subscriptions)

        connection = await self.wait()
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        self.add_subscriber(subscriber=subscriber, subscriptions=subscriptions)
        self.connect_subscriber(subscriber=subscriber, connection=connection)

        return subscriber

    async def _subscribe(self, subscriptions: Set[BaseSourceSubscription]) -> None:
        """Acquire locks for subscriptions and subscribe on the client."""
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
                raise TimeseriesManagerClosed() from e
            except Exception as e:
                _LOGGER.warning("Error subscribing on client", exc_info=True)
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("An error occurred while subscribing.") from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")
    
    async def bind_subscriber(
        self,
        subscriber: BaseSubscriber,
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
            await self._client.close()

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
                            "Dropping %i client subscriptions due to repeated "
                            "connection failures",
                            len(self._client.subscriptions)
                        )
                        callbacks = [lambda _: self._client.clear]
                        self.add_background_task(
                            self._client.unsubscribe,
                            self._client.subscriptions,
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
                        tg.start_soon(self._publish_messages, connection, self.exchange)
                        await asyncio.shield(connection.closing)
                except ClientClosed:
                    with suppress(Exception):
                        await connection.close(timeout=2)
                    _LOGGER.info("Exited manager because client is closed")
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
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="direct")

        # When the broker connection drops, the link between the broker and
        # subscribers is broken. The subscribers will wait for a new connection
        # and then re-declare all queues and bindings. All queues declared are
        # temporary for obvious reasons so when we have interruptions, the
        # subscribers have to race to re-declare their queues before any messages
        # are published otherwise those messages will not be routed and will be lost.
        
        # If we only had to worry about subscribers in a single process, we could
        # wait on the declarations before publishing anything. But, when we have
        # multiple processes spanning potentially multiple hosts, there is no
        # way to confirm all subscriber links have been re-established. So
        # instead, all we do is wait a couple of seconds. This should give the
        # subscribers enough time re-establish their link to the broker before
        # anything is published. However, if it takes longer than the waiting
        # period to re-declare the queues and bindings, events will be lost.
        await asyncio.sleep(2)

        source = self._source.source
        async for msg in self._client.get_messages():
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
        async for msg in self._client.get_dropped():
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

                # It is okay if this fails due to a transient error. The client
                # subscriptions have been dropped so their locks will not be
                # extended by this process anymore and will eventually expire
                # on the caching server.

                # However, there is small potential for data loss.
                # There at most a TTL gap between the time `release` fails to
                # the actual release due to expiration on the server. In that
                # TTL, another subscriber in a different process can subscribe
                # to the unreleased subscription. Assuming that process does not
                # also experience a transient error, the manager will try to
                # acquire the lock and see it is already held and (incorrectly)
                # assume the data is being streamed elsewhere. In actuality that
                # data is not being streamed. Therefore we have a window where
                # a subscriber expects to receive data it will never get. This
                # is, at most, 2.5 TTL. When the manager in the other process
                # polls its subscribers against the caching server, it will see
                # that client lock is no longer held and attempt to subscribe
                # on its own client.
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
                    self.add_background_task(
                        self._client.unsubscribe,
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
                    # This is where we have a chance for duplicate subscriptions
                    # in multiple processes. For polling operations, if a transient
                    # error occurs, we assume the subscription is needed and we
                    # stream the data. If the caching server goes down temporarily,
                    # all locks will be lost (or expire if there is some persistence).
                    # When the server becomes available, there is race between
                    # the process which owns a client subscription to re-acquire
                    # its lock (by extending) and the process which has a subscriber
                    # trying to stream data. If the process which does not own the
                    # client subscription goes first, it will poll the caching
                    # server and see that no process is streaming the subscription
                    # it wants. It will acquire a lock and subscribe on its client.
                    # In the meantime, the process which owned the client subscription
                    # will just continue to extend its lock not knowing that the
                    # other process is also streaming the same subscription. This
                    # is why subscribers must be the final gatekeeper for duplicate
                    # data. If clients follow the protocol and ensure data is
                    # sorted by timestamp it is trivial for a subscribers to just
                    # look at the last timestamp in a message and ensure they only
                    # forward messages where the timestamp is greater than the
                    # last timestamp they saw.
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