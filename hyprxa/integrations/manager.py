import asyncio
import itertools
import logging
import random
from collections.abc import Sequence
from contextlib import suppress
from contextvars import Context
from datetime import datetime
from typing import Callable, Dict, Set, Type

import anyio
from aiormq import Connection
from aiormq.abc import DeliveredMessage

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
from hyprxa.integrations.models import (
    BrokerStatus,
    ManagerInfo,
    Subscription,
    SubscriptionMessage
)
from hyprxa.integrations.protocols import Client, Subscriber
from hyprxa.timeseries import MongoTimeseriesHandler
from hyprxa.util.backoff import EqualJitterBackoff



_LOGGER = logging.getLogger("hyprxa.integrations")


class ClientManager:
    """Data integration manager backed by RabbitMQ.

    A client manager has 3 primary functions...
        1. Manage client subscriptions.
        2. Route messages from a client to subscribers.
        3. Store messages from a client in a database.

    ## Subscription Management
    The client manager uses a distributed locking backend to manage client
    subscriptions. hyprxa currently supports Redis and Memcached for locking
    backends. When the `subscribe` method is called, the manager will attempt to
    lock all subscriptions requested to this process. For all locks that it acquires,
    it will subscribe on the client. If a subscription lock is not acquired it
    is because another client in the cluster is already subscribed to that
    subscription. This allows hyprxa to scale without increasing demand on a data
    source.

    Subscription locks are periodically extended by the owning process to indicate
    that process is still streaming that subscription. If a subscription is dropped
    by the owning process' client, the lock will not be extended anymore. Another
    manager in the cluster may attempt to acquire the subscription lock if a
    subscriber in its pool requires it.

    Managers will unsubscribe from a client subscription when there is no subscriber
    in the cluster which requires that subscription.

    If the locking backend has momentary interruptions, there is a chance that
    two or more managers in the cluster could end up streaming the same subscription
    on their client leading to duplicate data. Subscriber implementations must
    guard against this. The simplest way to do this is to keep a reference to the
    last timestamp for each subscription. The subscriber should only yield a message
    if the timestamp for a message is greater than the last referenced timestamp.

    ## Message Routing
    Message routing is backed by RabbitMQ. When a managers `start` method is
    called, a background service is started which opens a connection to the
    broker and begins receiving messages from the client.
    
    When a message is received from the client, the source (provided in the
    `start` method) and the hash of the subscription (required in the
    `SubscriptionMessage` model) is used to create a unique routing key for
    that message. When a subscriber subscribes to a sequence of subscriptions
    a queue is declared on the exchange and bound to all subscriptions by
    generating an identical routing key. This way, any message from the client
    is published to the broker and routed directly to any subscribers in the
    cluster.

    If the connection to RabbitMQ drops, the link between the broker and subscriber
    is lost but the subscriber is not dropped (at least not initially). The
    manager will continuously try and re-establish the connection. When the
    connection is re-established the service will pick back up and begin to
    route messages to the subscriber again. While reconnecting, the client
    will continue to operate as usual and buffer messages on its data queue.
    These messages will be picked up by the manager and published on the broker.
    However, it is not guarenteed that any of these buffered messages make it
    to the subscriber. The manager will wait for 2 seconds before starting to
    publish messages again. This gives some time for subscribers to re-establish
    their connection to the broker before the buffered messages are published.
    If the subscribers do not re-establish their connection though, any messages
    published will not reach the subscriber.

    ## Message Storage
    When a message is picked up by the manager from the client, it is buffered
    on the storage handler as timeseries samples. Those samples are flushed to
    the database per the storage handlers settings.

    Args:
        factory: A callable that returns an `aiormq.Connection`.
        lock: The subscription lock.
        storage: The timeseries handler to write timeseries samples to the
            database.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
        max_buffered_messages: The maximum number of messages that can be buffered
            on the manager waiting to write to the database.
        subscription_timeout: The time to wait for the broker to be ready
            before rejecting the subscription request.
        reconnect_timeout: The time to wait for the broker to be ready
            before dropping an already connected subscriber.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
        max_failed: The maximum number of failed connection attempts before all
            subscribers and client subscriptions tied to this manager are dropped.
    """
    def __init__(
        self,
        factory: Callable[[], Connection],
        lock: BaseLock,
        storage: MongoTimeseriesHandler,
        exchange: str = "hyprxa.exchange.data",
        max_subscribers: int = 200,
        maxlen: int = 100,
        max_buffered_messages: int = 1000,
        subscription_timeout: float = 5,
        reconnect_timeout: float = 60,
        max_backoff: float = 3,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        self._factory = factory
        self._lock = lock
        self._storage = storage
        self._exchange = exchange
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._storage_queue: asyncio.Queue[SubscriptionMessage] = asyncio.Queue(maxsize=max_buffered_messages)
        self._subscription_timeout = subscription_timeout
        self._reconnect_timeout = reconnect_timeout
        self._backoff = EqualJitterBackoff(cap=max_backoff, initial=initial_backoff)
        self._max_failed = max_failed

        self._connection: Connection = None
        self._source: str = None
        self._client: Client = None
        self._subscriber: Type[Subscriber] = None
        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Future, Subscriber] = {}
        self._publishers: Dict[asyncio.Task, Subscriber] = {}
        self._ready: asyncio.Event = asyncio.Event()
        self._runner: asyncio.Task = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._subscribers_serviced = 0

    @property
    def closed(self) -> None:
        raise self._runner is None or self._runner.done()

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
    
    def start(self, source: str, client: Client, subscriber: Type[Subscriber]) -> None:
        """Start the manager and associate the instance with a client.
        
        Args:
            source: The name of the data source.
            client: The client connecting to the data source.
            subscriber: The subscriber type receiving client messages.
        """
        if not self.closed:
            return

        self._source = source
        self._client = client
        self._subscriber = subscriber

        runner: asyncio.Task = Context().run(self._loop.create_task, self._run())
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

    async def close(self) -> None:
        """Close the manager."""
        for fut in itertools.chain(
            self._subscribers.keys(),
            self._publishers.keys(),
            self._background
        ):
            fut.cancel()
        runner, self._runner = self._runner, None
        if runner is not None:
            runner.cancel()
        if self._client is not None and not self._client.closed:
            try:
                await self._client.close()
            finally:
                self._client.clear()
                self._source, self._client, self._subscriber = None, None, None

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
            raise SubscriptionLimitError(f"Max subscriptions reached ({self._max_subscribers})")
        
        subscriptions = set(subscriptions)

        connection = await self._wait_for_broker(self._subscription_timeout)
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self._subscriber_lost)
        self._subscribers[fut] = subscriber

        self._start_publisher(subscriber, connection)

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        self._subscribers_serviced += 1

        return subscriber

    async def _subscribe(self, subscriptions: Set[Subscription]) -> None:
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
                raise ManagerClosed() from e
            except Exception as e:
                _LOGGER.warning("Error subscribing on client", exc_info=True)
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("An error occurred while subscribing.") from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")

    def _subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        assert fut in self._subscribers
        subscriber = self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in %s", subscriber.__class__.__name__, exc_info=e)

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
        subscriber: Subscriber,
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
        
    async def _reconnect_publisher(self, subscriber: Subscriber) -> None:
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
        subscriber: Subscriber,
        connection: Connection,
        exchange: str
    ) -> None:
        """Link a RabbitMQ queue to a subscriber for message routing."""
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            subscriber.publish(data)
        
        channel = await connection.channel(publisher_confirms=False)
        try:
            await channel.exchange_declare(exchange=exchange, exchange_type="direct")
            declare_ok = await channel.queue_declare(exclusive=True)
            
            binds = [
                channel.queue_bind(
                    declare_ok.queue,
                    exchange=exchange,
                    routing_key=f"{self._source}-{hash(subscription)}"
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

    async def _run(self) -> None:
        """Manage background tasks for manager."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._manage_subscriptions)
                tg.start_soon(self._manage_broker_connection)
                tg.start_soon(self._store_messages)
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Manager failed", exc_info=True)
            raise

    async def _manage_broker_connection(self) -> None:
        """Manages broker connection for manager."""
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
                        "Dropping %i subscribers and client subscriptions due to "
                        "repeated connection failures",
                        len(self._subscribers)
                    )
                    for fut in self._subscribers.keys(): fut.cancel()
                    
                    fut = self._loop.create_task(self._client.unsubscribe(self._client.subscriptions))
                    fut.add_done_callback(lambda _: self._client.clear())
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
                
                continue
            
            else:
                attempts = 0
                self._connection = connection
                self._ready.set()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._publish_messages, connection, self._exchange)
                    await asyncio.shield(connection.closing)
            except ClientClosed:
                self._ready.clear()
                self._connection = None
                for fut in self._publishers.keys(): fut.cancel()
                with suppress(Exception):
                    await connection.close(timeout=2)
                _LOGGER.info("Exited manager because client is closed")
                raise
            except (Exception, anyio.ExceptionGroup):
                self._ready.clear()
                self._connection = None
                for fut in self._publishers.keys(): fut.cancel()
                with suppress(Exception):
                    await connection.close(timeout=2)
                _LOGGER.warning("Error in manager", exc_info=True)
            
            sleep = self._backoff.compute(attempts)
            _LOGGER.warning(
                "Manager unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)

    async def _publish_messages(self, connection: Connection, exchange: str) -> None:
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="direct")

        # There is no way we can guarentee that buffered messages on the client
        # will be routed to subscribers during a reconnect. Chances are that
        # some messages may be published to the exchange and discarded as unroutable
        # because the subscribers have not finished binding to the exchange. The
        # best we can do is wait a couple of seconds before starting to publish.
        # This should allow any subscribers to re-establsh their link to the
        # broker before we publish any buffered messages. But again, its not
        # guarenteed that we wont lose anything.
        await asyncio.sleep(2)

        async for msg in self._client.messages():
            await self._storage_queue.put(msg)
            routing_key = f"{self._source}-{hash(msg.subscription)}"
            await channel.basic_publish(
                msg.json().encode(),
                exchange=exchange,
                routing_key=routing_key
            )

    async def _store_messages(self) -> None:
        """Store messages in the timeseries database."""
        while True:
            msg = await self._storage_queue.get()
            self._storage_queue.task_done()
            await anyio.to_thread.run_sync(
                self._storage.publish,
                msg.to_samples(self._source),
                cancellable=True
            )

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
                            if not_subscribed.difference(subscriber.subscriptions) != not_subscribed:
                                fut.cancel()
                                _LOGGER.warning(
                                    "Subscriber dropped. Unable to pick up lost subscriptions",
                                    exc_info=True
                                )

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass