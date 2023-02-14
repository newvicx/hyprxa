import asyncio
import hashlib
import logging
import uuid
from datetime import datetime
from typing import List, Set

import anyio
from pymemcache import PooledClient as Memcached

from hyprxa.settings import memcached_settings, timeseries_manager_settings
from hyprxa.timeseries.models import BaseSourceSubscription, LockInfo



_LOGGER = logging.getLogger("hyprxa.integrations")


def memcached_poll(
    memcached: Memcached,
    subscriptions: List[BaseSourceSubscription],
    hashes: List[str]
) -> List[BaseSourceSubscription]:
    """Runs a GET command on a sequence of keys. Returns the subscriptions which
    exist.
    """
    try:
        results = [memcached.get(hash_) for hash_ in hashes]
    except Exception:
        _LOGGER.warning("Error in memcached client", exc_info=True)
        # For polling operations we assume the subscription exists or is still
        # needed so we return an empty set
        return set()
    else:
        return set([subscription for subscription, result in zip(subscriptions, results) if not result])


def memcached_release(
    memcached: Memcached,
    id_: bytes,
    hashes: List[str]
) -> None:
    """Runs a GET command then deletes keys with a matching lock ID."""
    try:
        results = [memcached.get(hash_) for hash_ in hashes]
        hashes = [hash_ for hash_, result in zip(hashes, results) if result == id_]
        if hashes:
            memcached.delete_many(hashes)
    except Exception:
        _LOGGER.warning("Error in memcached client", exc_info=True)


class SubscriptionLock:
    """Distributed lock for client subscriptions backed by Memcached.
    
    Args:
        memcached: The memcached client.
        ttl: The time (in seconds) to acquire and extend locks for.
        max_workers: The maximum number of threads that can execute memcached commands.
    """
    def __init__(
        self,
        memcached: Memcached,
        ttl: int = 5,
        max_workers: int = 4
    ) -> None:
        self._memcached = memcached
        self._ttl = ttl
        self._id = uuid.uuid4().hex
        self._limiter = anyio.CapacityLimiter(max_workers)

        self._created = datetime.now()

    @property
    def info(self) -> LockInfo:
        """Returns current information on the lock."""
        return LockInfo(
            name=self.__class__.__name__,
            backend="memcached",
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
        )

    @property
    def ttl(self) -> float:
        """The TTL used for locks in seconds."""
        return self._ttl
    
    @classmethod
    def from_settings(cls) -> "SubscriptionLock":
        memcached = memcached_settings.get_client()
        return cls(
            memcached=memcached,
            ttl=timeseries_manager_settings.lock_ttl,
            max_workers=memcached_settings.max_pool_size
        )

    async def acquire(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> Set[BaseSourceSubscription]:
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
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            id_ = self._id
            ttl = self._ttl
            dispatch = [
                anyio.to_thread.run_sync(
                    self._memcached.add,
                    hash_,
                    id_,
                    ttl,
                    limiter=self._limiter
                )
                for hash_ in hashes
            ]
            results = await asyncio.gather(*dispatch)
        except Exception:
            _LOGGER.warning("Error in memcached client", exc_info=True)
            # With the Memcached lock we dont have the concept of a transaction
            # so we cant know if some locks were acquired so we call release
            # just in case
            await self.release(subscriptions)
            raise
        else:
            return set([subscription for subscription, stored in zip(subscriptions, results) if stored])

    async def register(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> None:
        """Register subscriptions tied to a subscriber.

        Args:
            subscriptions: A sequence of subscriptions to register.
        
        Raises:
            None: Exceptions should be logged and swallowed.
        """
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        try:
            id_ = self._id
            ttl = self._ttl
            values = {hash_: id_ for hash_ in hashes}
            await anyio.to_thread.run_sync(
                self._memcached.set_many,
                values,
                ttl,
                limiter=self._limiter
            )
        except Exception:
            _LOGGER.warning("Error in memcached client", exc_info=True)

    async def release(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> None:
        """Release the locks for subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to release an owned lock for.

        Returns:
            subscriptions: The subscriptions which the lock was released.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        id_ = self._id.encode()
        await anyio.to_thread.run_sync(
            memcached_release,
            self._memcached,
            id_,
            hashes,
            limiter=self._limiter
        )

    async def extend_client(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> None:
        """Extend the locks on client subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend an owned lock for.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            id_ = self._id
            ttl = self._ttl
            values = {hash_: id_ for hash_ in hashes}
            await anyio.to_thread.run_sync(
                self._memcached.set_many,
                values,
                ttl,
                limiter=self._limiter
            )
        except Exception:
            _LOGGER.warning("Error in memcached client", exc_info=True)

    async def extend_subscriber(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> None:
        """Extend the registration on subscriber subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend a registration for.

        Raises:
            None: Exceptions should be logged and swallowed.
        """
        await self.register(subscriptions)
    
    async def client_poll(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> Set[BaseSourceSubscription]:
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
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )

    async def subscriber_poll(
        self,
        subscriptions: Set[BaseSourceSubscription]
    ) -> Set[BaseSourceSubscription]:
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
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )
    
    def subscriber_key(self, subscription: BaseSourceSubscription) -> str:
        """Return the subscriber key from the subscription hash.
        
        Args:
            subscription: The subscription to hash.
        
        Returns:
            hash: A hash derived from the subscription hash.
        """
        o = str(hash(subscription)).encode()
        return str(int(hashlib.shake_128(o).hexdigest(16), 16))