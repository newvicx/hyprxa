import logging
import re
import uuid
from datetime import datetime
from typing import List, Set

try:
    from redis.asyncio import Redis
except ImportError:
    pass

from hyprxa.caching import memo
from hyprxa.integrations.base import BaseLock
from hyprxa.integrations.models import LockInfo, Subscription



_LOGGER = logging.getLogger("hyprxa.integrations")
_RELEASE_LOCK = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""
_LUA_SCRIPTS = {"release": _RELEASE_LOCK}


@memo
async def register_lua_script(script: str, _redis: Redis) -> str:
    script = re.sub(r'^\s+', '', script, flags=re.M).strip()
    return await _redis.script_load(script)


async def redis_poll(
    redis: Redis,
    subscriptions: List[Subscription],
    hashes: List[str]
) -> List[Subscription]:
    """Runs a GET command on a sequence of keys. Returns the subscriptions which
    dont exist.
    """
    try:
        async with redis.pipeline(transaction=True) as pipe:
            for hash_ in hashes:
                pipe.get(hash_)
            results = await pipe.execute()
    except Exception:
        _LOGGER.warning("Error in redis client", exc_info=True)
        # For polling operations we assume the subscription exists or is still
        # needed so we return an empty set
        return set()
    else:
        return set([subscription for subscription, result in zip(subscriptions, results) if not result])


class RedisLock(BaseLock):
    """Lock implementation with Redis backend.
    
    Args:
        redis: The redis client.
        ttl: The time in milliseconds to acquire and extend locks for.
    """
    def __init__(self, redis: "Redis", ttl: int = 5000) -> None:
        self._redis = redis
        self._ttl = ttl
        self._id = uuid.uuid4().hex

        self._created = datetime.now()

    @property
    def info(self) -> LockInfo:
        return LockInfo(
            name=self.__class__.__name__,
            backend="redis",
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
        )

    @property
    def ttl(self) -> float:
        return self._ttl/1000

    async def acquire(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                ttl = self._ttl
                for hash_ in hashes:
                    pipe.set(hash_, id_, px=ttl, nx=True)
                results = await pipe.execute()
        except Exception:
            _LOGGER.warning("Error in redis client", exc_info=True)
            raise
        else:
            return set([subscription for subscription, result in zip(subscriptions, results) if result])

    async def register(self, subscriptions: Set[Subscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                ttl = self._ttl
                for hash_ in hashes:
                    pipe.set(hash_, id_, px=ttl)
                await pipe.execute()
        except Exception:
            _LOGGER.warning("Error in redis client", exc_info=True)

    async def release(self, subscriptions: Set[Subscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        script = _LUA_SCRIPTS["release"]
        try:
            sha = await register_lua_script(script, self._redis)
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                for hash_ in hashes:
                    args = (hash_, id_)
                    pipe.evalsha(sha, 1, *args)
                await pipe.execute()
        except Exception:
            _LOGGER.warning("Error in redis client", exc_info=True)

    async def extend_client(self, subscriptions: Set[Subscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                ttl = self._ttl
                for hash_ in hashes:
                    pipe.set(hash_, id_, px=ttl)
                await pipe.execute()
        except Exception:
            _LOGGER.warning("Error in redis client", exc_info=True)

    async def extend_subscriber(self, subscriptions: Set[Subscription]) -> None:
        await self.register(subscriptions)

    async def client_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        return await redis_poll(self._redis, subscriptions, hashes)

    async def subscriber_poll(self, subscriptions: Set[Subscription]) -> Set[Subscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        return await redis_poll(self._redis, subscriptions, hashes)