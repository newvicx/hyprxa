import asyncio
import logging
import uuid
from datetime import datetime
from typing import List, Set

import anyio
from pymemcache import PooledClient as Memcached

from hyprxa.integrations.base import BaseLock
from hyprxa.integrations.models import LockInfo, Subscription



_LOGGER = logging.getLogger("hyprxa.integrations")


def memcached_poll(
    memcached: Memcached,
    subscriptions: List[Subscription],
    hashes: List[str]
) -> List[Subscription]:
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


class MemcachedLock(BaseLock):
    """Lock implementation with Memcached backend.
    
    Args:
        memcached: The memcached client.
        ttl: The time in milliseconds to acquire and extend locks for.
        max_workers: The maximum number of threads that can execute memcached commands.
    """
    def __init__(self, memcached: Memcached, ttl: int = 5000, max_workers: int = 4) -> None:
        self._memcached = memcached
        self._ttl = int(ttl/1000)
        self._id = uuid.uuid4().hex
        self._limiter = anyio.CapacityLimiter(max_workers)

        self._created = datetime.now()

    @property
    def info(self) -> LockInfo:
        return LockInfo(
            name=self.__class__.__name__,
            backend="memcached",
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
        )

    @property
    def ttl(self) -> float:
        return self._ttl

    async def acquire(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
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

    async def register(self, subscriptions: Set[Subscription]) -> None:
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

    async def release(self, subscriptions: Set[Subscription]) -> None:
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

    async def extend_client(self, subscriptions: Set[Subscription]) -> None:
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

    async def extend_subscriber(self, subscriptions: Set[Subscription]) -> None:
        await self.register(subscriptions)
    
    async def client_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )

    async def subscriber_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )