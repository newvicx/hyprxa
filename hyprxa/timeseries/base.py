import asyncio
from collections.abc import AsyncIterable
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict, Set

from hyprxa.timeseries.exceptions import IntegrationClosed
from hyprxa.timeseries.models import (
    BaseSourceSubscription,
    ConnectionInfo,
    DroppedSubscriptions,
    IntegrationInfo,
    SubscriptionMessage
)



class BaseConnection:
    """Base implementation for a connection.
    
    Connections implement the protocol to retrieve data from a source, parse
    the data, and package it into a `SubscriptionMessage` for the integration.
    Connections are always slaves to a integration and should only be created in
    the context of a integration.
    """
    def __init__(self) -> None:
        self._subscriptions: Set[BaseSourceSubscription] = set()
        self._data_queue: asyncio.Queue = None
        self._online: bool = False
        self._started: asyncio.Event = asyncio.Event()

        self._created = datetime.utcnow()
        self._total_published = 0

    @property
    def info(self) -> ConnectionInfo:
        """Returns current information on the connection."""
        return ConnectionInfo(
            name=self.__class__.__name__,
            online=self.online,
            created=self._created,
            uptime=(datetime.utcnow() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self._subscriptions)
        )

    @property
    def online(self) -> bool:
        """Returns `True` if the connection is 'online' and is allowed to pubish
        data to the integration.
        """
        return self._online

    @property
    def subscriptions(self) -> Set[BaseSourceSubscription]:
        """Return a set of the subscriptions for this connections."""
        return self._subscriptions

    def toggle(self) -> None:
        """Toggle the online status of the connection."""
        self._online = not self._online

    async def publish(self, data: SubscriptionMessage) -> None:
        """Publish data to the integration."""
        await self._data_queue.put(data)
        self._total_published += 1

    async def run(self, *args: Any, **kwargs: Any) -> None:
        """Main implementation for the connection.
        
        This method should receive/retrieve, parse, and validate data from the
        source which it is connecting to.
        
        When the connection status is 'online' data may be published to the integration.
        """
        raise NotImplementedError()

    async def start(
        self,
        subscriptions: Set[BaseSourceSubscription],
        data_queue: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> asyncio.Task:
        """Start the connection.

        Args:
            subscriptions: A set of subscriptions to connect to at the data
                source.
            data_queue: A queue where processed data is put.
        
        Returns:
            fut: The connection task.
        """
        raise NotImplementedError()


class BaseIntegration:
    """Base implementation for a data source integration.

    Integrations interface between a `TimeseriesManager` and a data source. A data
    source could be a REST API, CSV file, or database; it doesn't matter. A integration
    abstracts away the mechanism of the data integration from the manager and
    provides a consistent interface to proxy data. Integrations do this through a
    'connection'. A connection is where actual I/O and data processing occur,
    a integration manages those connections, adding them when needed for
    subscriptions and tearing them down when no longer needed.

    A hyprxa compliant integration/connection implementation must provide certain
    guarentees...

        - Integrations must only send messages containing unique timestamped values in
        monotonically increasing order
        - `TimestampedValue`(s) must be sorted within a message.

    How this is done is entirely up to the implementation. Some data sources
    may already provide this guarentee but it is the responsibility of the
    integration to ensure this is met.
    
    Args:
        max_buffered_messages: The max length of the data queue for the integration.
    """
    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self._connections: Dict[asyncio.Task, BaseConnection] = {}
        self._data: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self._dropped: asyncio.Queue = asyncio.Queue()

        self._created = datetime.utcnow()
        self._connections_serviced = 0

    @property
    def capacity(self) -> int:
        """Returns an integer indicating how many more subscriptions this integration
        can support.
        """
        raise NotImplementedError()

    @property
    def closed(self) -> bool:
        """Returns `True` if integration is closed. A closed integration cannot accept
        new subscriptions.
        """
        raise NotImplementedError()

    @property
    def info(self) -> IntegrationInfo:
        """Returns current information on the integration."""
        return IntegrationInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            data_queue_size=self._data.qsize(),
            dropped_connection_queue_size=self._dropped.qsize(),
            created=self._created,
            uptime=(datetime.utcnow() - self._created).total_seconds(),
            active_connections=len(self._connections),
            active_subscriptions=len(self.subscriptions),
            subscription_capacity=self.capacity,
            total_connections_serviced=self._connections_serviced,
            connection_info=[connection.info for connection in self._connections.values()]
        )

    @property
    def subscriptions(self) -> Set[BaseSourceSubscription]:
        """Returns a set of the subscriptions from all connections."""
        subscriptions = set()
        for fut, connection in self._connections.items():
            if not fut.done():
                subscriptions.update(connection.subscriptions)
        return subscriptions

    def clear(self) -> None:
        """Clear all buffered data on the integration."""
        for queue in (self._data, self._dropped):
            try:
                while True:
                    queue.get_nowait()
                    queue.task_done()
            except asyncio.QueueEmpty:
                pass

    async def close(self) -> None:
        """Close the integration instance and shut down all connections.
        
        `close` must be idempotent.
        """
        raise NotImplementedError()

    async def get_dropped(self) -> AsyncIterable[DroppedSubscriptions]:
        """Receive messages for dropped connections."""
        while not self.closed:
            msg = await self._dropped.get()
            self._dropped.task_done()
            yield msg
        else:
            raise IntegrationClosed()
    
    async def get_messages(self) -> AsyncIterable[SubscriptionMessage]:
        """Receive incoming messages from all connections."""
        while not self.closed:
            msg = await self._data.get()
            self._data.task_done()
            yield msg
        else:
            raise IntegrationClosed()

    async def subscribe(self, subscriptions: Set[BaseSourceSubscription]) -> bool:
        """Subscribe to a set of subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            bool: If `True`, all subscriptions were subscribed to. If `False`,
                no subscriptions were subscribed to.
        """
        raise NotImplementedError()

    async def unsubscribe(self, subscriptions: Set[BaseSourceSubscription]) -> bool:
        """Unsubscribe from from a set of subscriptions.
        
        Args:
            subscriptions: The subscriptions to unsubscribe from.

        Returns:
            bool: If `True`, all subscriptions were unsubscribed from. If `False`,
                no subscriptions were unsubscribed from.
        """
        raise NotImplementedError()

    def add_connection(self, fut: asyncio.Task, connection: BaseConnection) -> None:
        """Add a running connection to the integration."""
        self._connections[fut] = connection
        self._connections_serviced += 1

    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback after connections have stopped."""
        assert fut in self._connections
        connection = self._connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        # If a connection was cancelled by the integration and the subscriptions were
        # replaced through another connection, subscriptions will be empty set.
        # But, `unsubscribe` will lead to non-empty subscriptions.
        msg = DroppedSubscriptions(
            subscriptions=connection.subscriptions.difference(self.subscriptions),
            error=e
        )
        self._dropped.put_nowait(msg)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass