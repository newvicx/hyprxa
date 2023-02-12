import asyncio
from collections.abc import AsyncIterable
from types import TracebackType
from typing import Any, Deque, Protocol, Set, Type

from hyprxa.integrations.models import (
    ClientInfo,
    ConnectionInfo,
    DroppedSubscriptions,
    LockInfo,
    SubscriberCodes,
    SubscriberInfo,
    Subscription,
    SubscriptionMessage
)



class Client(Protocol):
    @property
    def capacity(self) -> int:
        """Returns an integer indicating how many more subscriptions this client
        can support.
        """
        ...

    @property
    def closed(self) -> bool:
        """Returns `True` if client is closed. A closed client cannot accept
        new subscriptions.
        """
        ...
    
    @property
    def subscriptions(self) -> Set[Subscription]:
        """Returns a set of the subscriptions from all connections."""
        ...

    @property
    def info(self) -> ClientInfo:
        """Returns current information on the client."""
        ...

    def clear(self) -> None:
        """Clear all buffered data on the client."""
        ...
    
    async def close(self) -> None:
        """Close the client instance and shut down all connections."""
        ...

    async def dropped(self) -> AsyncIterable[DroppedSubscriptions]:
        """Receive messages for dropped connections."""
        yield

    async def messages(self) -> AsyncIterable[SubscriptionMessage]:
        """Receive incoming messages from all connections."""
        yield

    async def subscribe(self, subscriptions: Set[Subscription]) -> bool:
        """Subscribe to a set of subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            bool: If `True`, all subscriptions were subscribed to. If `False`,
                no subscriptions were subscribed to.
        """
        ...

    async def unsubscribe(self, subscriptions: Set[Subscription]) -> bool:
        """Unsubscribe from from a set of subscriptions.
        
        Args:
            subscriptions: The subscriptions to unsubscribe from.

        Returns:
            bool: If `True`, all subscriptions were unsubscribed from. If `False`,
                no subscriptions were unsubscribed from.
        """
        ...

    def add_connection(self, fut: asyncio.Task, connection: "Connection") -> None:
        """Add a running connection to the client."""
        ...

    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback after connections have stopped."""
        ...


class Connection(Protocol):
    @property
    def info(self) -> ConnectionInfo:
        """Returns current information on the connection."""
        ...

    @property
    def online(self) -> bool:
        """Returns `True` if the connection is 'online' and is allowed to pubish
        data to the client.
        """
        ...

    @property
    def subscriptions(self) -> Set[Subscription]:
        """Return a set of the subscriptions for this connections."""
        ...

    def toggle(self) -> None:
        """Toggle the online status of the connection."""
        ...

    async def publish(self, data: SubscriptionMessage) -> None:
        """Publish data to the client."""
        ...

    async def run(self, *args: Any, **kwargs: Any) -> None:
        """Main implementation for the connection.
        
        This method should receive/retrieve, parse, and validate data from the
        source which it is connecting to.
        
        When the connection status is 'online' data may be published to the client.
        """
        ...

    async def start(
        self,
        subscriptions: Set[Subscription],
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
        ...


class Subscriber(Protocol):
    @property
    def data(self) -> Deque[str]:
        """Returns the data buffer for this subscriber."""
        ...

    @property
    def info(self) -> SubscriberInfo:
        """Returns current information on the subscriber."""
        ...
        

    @property
    def stopped(self) -> bool:
        """Returns `True` if subscriber cannot be iterated over."""
        ...

    @property
    def subscriptions(self) -> Set[Subscription]:
        """Returns a set of the subscriptions for this subscriber."""
        ...

    def publish(self, data: bytes) -> None:
        """Publish data to the subscriber.
        
        This method is called by the manager.
        """
        ...
    
    def start(self, subscriptions: Set[Subscription], maxlen: int) -> asyncio.Future:
        """Start the subscriber.
        
        This method is called by the manager.
        """
        ...

    def stop(self, e: Exception | None) -> None:
        """Stop the subscriber."""
        ...

    async def wait(self) -> SubscriberCodes:
        """Wait for new data to be published."""
        ...

    async def wait_for_stop(self) -> None:
        """Waits for the subscriber to be stopped.
        
        If a call to this method is cancelled it must not stop the subscriber.
        """
        ...

    async def __aiter__(self) -> AsyncIterable[str]:
        ...

    def __enter__(self) -> "Subscriber":
        ...

    def __exit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        ...


class Lock:
    @property
    def info(self) -> LockInfo:
        """Returns current information on the lock."""
        ...

    @property
    def ttl(self) -> float:
        """The TTL used for locks in seconds."""
        ...

    async def acquire(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        """Acquire a lock for a subscription tied to a client.
        
        Args:
            subscriptions: A sequence of subscriptions to try and lock to this
                process.

        Returns:
            subscriptions: The subscriptions for which a lock was successfully
                acquired.
        
        Raises:
            Exception: None of the locks were acquired or a best effort was made
                to ensure no locks were acquired.
        """
        ...

    async def register(self, subscriptions: Set[Subscription]) -> None:
        """Register subscriptions tied to a subscriber.

        Args:
            subscriptions: A sequence of subscriptions to register.
        
        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    async def release(self, subscriptions: Set[Subscription]) -> None:
        """Release the locks for subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to release an owned lock for.

        Returns:
            subscriptions: The subscriptions which the lock was released.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    async def extend_client(self, subscriptions: Set[Subscription]) -> None:
        """Extend the locks on client subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend an owned lock for.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    async def extend_subscriber(self, subscriptions: Set[Subscription]) -> None:
        """Extend the registration on subscriber subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend a registration for.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    async def client_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        """Poll subscriptions tied to the manager's client.
        
        This method returns subscriptions which can be unsubscribed from.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                is streaming data for.
        
        Returns:
            subscriptions: The subscriptions that can unsubscribed from.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    async def subscriber_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        """Poll subscriptions tied to the managers subscribers.
        
        This method returns subscriptions which are not being streamed by a manager
        in the cluster. A manager which owns the subscriber may choose to
        subscribe to the missing subscriptions (after it acquires a lock), or
        stop the subscriber.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                requires data to be streaming for.

        Returns:
            subscriptions: The subscriptions that are not being streamed anywhere
                in the cluster.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        ...

    def subscriber_key(self, subscription: Subscription) -> str:
        """Return the subscriber key from the subscription hash.
        
        Args:
            subscription: The subscription to hash.

        Returns:
            hash: A hash derived from the subscription hash.
        """
        ...