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

from hyprxa.base import (
    BaseBroker,
    BaseSubscriber,
    SubscriptionLimitError
)
from hyprxa.caching import singleton
from hyprxa.settings import rabbitmq_settings, timeseries_manager_settings
from hyprxa.sources import Source
from hyprxa.timeseries.base import BaseClient
from hyprxa.timeseries.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionError,
    SubscriptionLockError
)
from hyprxa.timeseries.handler import MongoTimeseriesHandler
from hyprxa.timeseries.lock import SubscriptionLock
from hyprxa.timeseries.models import (
    BaseSourceSubscription,
    ManagerInfo,
    SubscriptionMessage
)



_LOGGER = logging.getLogger("hyprxa.integrations")


class TimeseriesManager(BaseBroker):
    def __init__(
        self,
        source: Source,
        lock: SubscriptionLock,
        storage: MongoTimeseriesHandler,
        *args: Any,
        max_buffered_messages: int = 1000,
        max_failed: int = 15,
        **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
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
    def info(self) -> ManagerInfo:
        return ManagerInfo(
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
            client_info=self._client.info,
            lock_info=self._lock.info,
            total_published_messages=self._total_published,
            total_stored_messages=self._total_stored
        )

    @classmethod
    @singleton
    def from_environment(cls, source: Source) -> "TimeseriesManager":
        storage = MongoTimeseriesHandler.from_settings()
        lock = SubscriptionLock.from_settings()
        factory = rabbitmq_settings.get_factory()
        manager = cls(
            source=source,
            lock=lock,
            storage=storage,
            factory=factory,
            exchange=timeseries_manager_settings.exchange,
            max_buffered_messages=timeseries_manager_settings.max_buffered_messages,
            max_subscribers=timeseries_manager_settings.max_subscribers,
            maxlen=timeseries_manager_settings.maxlen,
            subscription_timeout=timeseries_manager_settings.subscription_timeout,
            reconnect_timeout=timeseries_manager_settings.reconnect_timeout,
            max_backoff=timeseries_manager_settings.max_backoff,
            initial_backoff=timeseries_manager_settings.initial_backoff,
            max_failed=timeseries_manager_settings.max_failed
        )
        manager.start()
        return manager
        
    def start(self) -> None:
        self._client, self._subscriber = self._source()
        super().start()

    def close(self) -> None:
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

    async def subscribe(self, subscriptions: Sequence[BaseSourceSubscription]) -> BaseSubscriber:
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(f"Max subscriptions reached ({self._max_subscribers})")
        
        subscriptions = set(subscriptions)

        connection = await self.wait()
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        self.add_subscriber(subscriber=subscriber, subscriptions=subscriptions)
        self.connect_subscriber(subscriber=subscriber, connection=connection)

        return subscriber
    
    async def bind_subscriber(
        self,
        subscriber: BaseSubscriber,
        channel: Channel,
        declare_ok: commands.Queue.DeclareOk,
    ) -> None:
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
                raise ManagerClosed() from e
            except Exception as e:
                _LOGGER.warning("Error subscribing on client", exc_info=True)
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("An error occurred while subscribing.") from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")
            
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
                    
                    if attempts >= self._max_failed:
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
                    
                    continue
                
                else:
                    attempts = 0
                    self.set_connection(connection)
                
                try:
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._publish_messages, connection, self.exchange)
                        await asyncio.shield(connection.closing)
                except ClientClosed:
                    self.remove_connection()
                    with suppress(Exception):
                        await connection.close(timeout=2)
                    _LOGGER.info("Exited manager because client is closed")
                    raise
                except (Exception, anyio.ExceptionGroup):
                    self.remove_connection()
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

    async def _publish_messages(self, connection: Connection, exchange: str) -> None:
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="direct")

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
            await anyio.to_thread.run_sync(
                self._storage.publish,
                msg.to_samples(source),
                cancellable=True
            )
            self._total_stored += 1

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