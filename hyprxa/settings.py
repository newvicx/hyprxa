import inspect
import logging
import logging.config
import pathlib
import secrets
import threading
from typing import Callable, List

import yaml
from aiormq import Connection
from motor.motor_asyncio import AsyncIOMotorClient
from pymemcache import PooledClient as Memcached
from pymongo import MongoClient
from pydantic import (
    AmqpDsn,
    AnyHttpUrl,
    BaseSettings,
    Field,
    FilePath,
    MongoDsn,
    SecretStr,
    StrictStr,
    confloat,
    conint
)

from hyprxa.auth.models import TokenHandler
from hyprxa.base.manager import BaseManager
from hyprxa.events.handler import EventWorker, MongoEventHandler
from hyprxa.events.manager import EventManager
from hyprxa.timeseries.sources import Source
from hyprxa.timeseries.handler import MongoTimeseriesHandler
from hyprxa.timeseries.lock import SubscriptionLock
from hyprxa.timeseries.manager import TimeseriesManager
from hyprxa.timeseries.handler import TimeseriesWorker
from hyprxa.util.defaults import DEFAULT_APPNAME, DEFAULT_DATABASE
from hyprxa.util.formatting import format_docstring
from hyprxa.util.logging import cast_logging_level
from hyprxa.util.mongo import MongoWorker



_BM = inspect.signature(BaseManager).parameters
_EM = inspect.signature(EventManager).parameters
_LK = inspect.signature(SubscriptionLock).parameters
_MW = inspect.signature(MongoWorker).parameters
_TH = inspect.signature(MongoTimeseriesHandler).parameters
_TM = inspect.signature(TimeseriesManager).parameters


class HyprxaSettings(BaseSettings):
    secret_key: SecretStr = Field(
        default=secrets.token_hex(32),
        description=format_docstring("""The secret key used to sign all JWT's
        issued by a `TokenHandler`. Defaults to a random 32 bytes hex value
        (secrets.token_hex(32)). This should be set as an env variable outside
        of testing otherwise a new secret key will be used on each restart""")
    )
    token_expire: int = Field(
        default=1800,
        description=format_docstring("""The expiration time of token (in seconds)
        after it is generated. Defaults to `1800` (30 minutes)""")
    )
    hash_algorithm: str = Field(
        default="HS256",
        description=format_docstring("""The hashing algorithm used to hash the
        secret key when creating a token. Defaults to 'HS256'""")
    )
    allow_origins: List[str] = Field(
        default=["http://localhost*", "https://localhost*"],
        description=format_docstring("""The allowed origins for CORS middleware.
        Defaults to 'localhost' for both HTTP and HTTPS.""")
    )
    admin_scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The admin scopes for the application.
        Allows for read/write and diagnostic information.""")
    )
    admin_any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the admin
        scopes is authorized for admin privilages. If `False` user needs all
        scopes. Defaults to `True`""")
    )
    admin_raise_on_no_scopes: bool = Field(
        default=True,
        description=format_docstring("""If `True`, and no admin scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires admin scopes. Defaults to `True`""")
    )
    write_scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The write scopes for the application.
        Allows for read/write operations.""")
    )
    write_any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the write
        scopes is authorized for write privilages. If `False` user needs all
        scopes. Defaults to `True`""")
    )
    write_raise_on_no_scopes: bool = Field(
        default=True,
        description=format_docstring("""If `True`, and no write scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires write scopes. Defaults to `True`""")
    )
    read_scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The read scopes for the application.
        Allows for read operations.""")
    )
    read_any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the read
        scopes is authorized for read privilages. If `False` user needs all
        scopes. Defaults to `True`""")
    )
    read_raise_on_no_scopes: bool = Field(
        default=False,
        description=format_docstring("""If `True`, and no read scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires read scopes. Defaults to `False`""")
    )
    token_path: str = Field(
        default="/token",
        description=format_docstring("""The path to the acquire an access token.
        Defaults to '/token'""")
    )

    def get_token_handler(self) -> TokenHandler:
        return TokenHandler(
            key=self.secret_key.get_secret_value(),
            expire=self.token_expire,
            algorithm=self.hash_algorithm
        )

    class Config:
        env_file=".env"
        env_prefix="hyprxa_"


class MongoSettings(BaseSettings):
    connection_uri: MongoDsn = Field(
        default="mongodb://localhost:27017/",
        description=format_docstring("""The connection uri to the MongoDB server.
        Defaults to 'mongodb://localhost:27017/'""")
    )
    heartbeat: conint(gt=0) = Field(
        default=20_000,
        description=format_docstring("""The number of milliseconds between
        periodic server checks. Defaults to `20000` (20 seconds)""")
    )
    server_selection_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstring("""Controls how long (in milliseconds) the
        driver will wait to find an available, appropriate server to carry out a
        database operation; while it is waiting, multiple server monitoring
        operations may be carried out, each controlled by `connect_timeout`.
        Defaults to `10000` (10 seconds).""")
    )
    connect_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstring("""Controls how long (in milliseconds) the
        driver will wait during server monitoring when connecting a new socket
        to a server before concluding the server is unavailable. Defaults to
        `10000` (10 seconds)""")
    )
    socket_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstring("""Controls how long (in milliseconds) the
        driver will wait for a response after sending an ordinary (non-monitoring)
        database operation before concluding that a network error has occurred.
        Defaults to `10000` (10 seconds)""")
    )
    timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstring("""Controls how long (in milliseconds)
        the driver will wait when executing an operation (including retry
        attempts) before raising a timeout error. Defaults to `10000` (10 seconds)""")
    )
    max_pool_size: conint(ge=0) = Field(
        default=25,
        description=format_docstring("""The maximum allowable number of concurrent
        connections to each connected server. Requests to a server will block if
        there are `max_pool_size` outstanding connections to the requested server.
        Can be 0, in which case there is no limit on the number of concurrent
        connections. Defaults to `25`""")
    )
    appname: str = Field(
        default=DEFAULT_APPNAME,
        description=format_docstring("""The name of the application that created
        the client instance. The server will log this value upon establishing
        each connection. It is also recorded in the slow query log and profile
        collections. Defaults to '{}'""".format(DEFAULT_APPNAME))
    )

    def get_async_client(self) -> AsyncIOMotorClient:
        return AsyncIOMotorClient(
            self.connection_uri,
            heartbeatFrequencyMS=self.heartbeat,
            serverSelectionTimeoutMS=self.server_selection_timeout,
            connectTimeoutMS=self.connect_timeout,
            socketTimeoutMS=self.socket_timeout,
            timeoutMS=self.timeout,
            maxPoolSize=self.max_pool_size,
            appname=self.appname
        )
    
    def get_client(self) -> MongoClient:
        return MongoClient(
            self.connection_uri,
            heartbeatFrequencyMS=self.heartbeat,
            serverSelectionTimeoutMS=self.server_selection_timeout,
            connectTimeoutMS=self.connect_timeout,
            socketTimeoutMS=self.socket_timeout,
            timeoutMS=self.timeout,
            maxPoolSize=self.max_pool_size,
            appname=self.appname
        )

    class Config:
        env_file=".env"
        env_prefix="mongodb_"


class RabbitMQSettings(BaseSettings):
    connection_uri: AmqpDsn = Field(
        default="amqp://guest:guest@localhost:5672/",
        description=format_docstring("""The connection uri to the RabbitMQ server.
        Defaults to 'amqp://guest:guest@localhost:5672/'""")
    )

    class Config:
        env_file=".env"
        env_prefix="rabbitmq_"

    def get_factory(self) -> Callable[[], Connection]:
        """Get a connection factory to the RabbitMQ server."""
        return lambda: Connection(self.connection_uri)


class MemcachedSettings(BaseSettings):
    connection_uri: str = Field(
        default="localhost:11211",
        description=format_docstring("""A `host:port` string to Memcached server.
        Defaults to 'localhost:11211'""")
    )
    connect_timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstring("""Controls how long (in seconds) the driver
        will wait to connect a new socket to a server. Defaults to `10`""")
    )
    timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstring("""Controls how long (in seconds) the driver
        will wait for send or recv calls on the socket connected to memcached.
        Defaults to `10`""")
    )
    max_pool_size: conint(gt=0) = Field(
        default=6,
        description=format_docstring("""The maximum number of concurrent connections
        to the memcached server per client. Defaults to `6`""")
    )
    pool_idle_timeout: confloat(gt=10) = Field(
        default=60,
        description=format_docstring("""The time (in seconds) before an unused
        pool connection is discarded. Defaults to `60`""")
    )

    def get_client(self) -> Memcached:
        """Configure a `PooledClient` instance from settings."""
        return Memcached(
            self.connection_uri,
            connect_timeout=self.connect_timeout,
            timeout=self.timeout,
            no_delay=False,
            max_pool_size=self.max_pool_size,
            pool_idle_timeout=self.pool_idle_timeout
        )
    
    def get_limiter(self) -> threading.Semaphore:
        """Configure a limiter from settings.
        
        The limiter prevents a `RuntimeError` in multithreaded contexts if more
        than `max_pool_size` threads try and get a connection from the pool
        concurrently. The limiter will block the thread if the limit is reached.
        """
        return threading.Semaphore(self.max_pool_size)

    class Config:
        env_file=".env"
        env_prefix="memcached_"


class SentrySettings(BaseSettings):
    enabled: bool = Field(
        default=True,
        description=format_docstring("""Controls whether the SDK is enabled for
        this runtime. If enabled, `dsn` must be specified. Defaults to `True`""")
    )
    dsn: AnyHttpUrl | None = Field(
        default=None,
        description=("""Tells the SDK where to send the events. If this value
        is not provided, the SDK will not send any events. Defaults to `None`""")
    )
    sample_rate: confloat(ge=0, le=1) = Field(
        default=1,
        description=format_docstring("""A number between 0 and 1, controlling
        the percentage chance a given event will be sent to Sentry (0 represents
        0% while 1 represents 100%). Events are picked randomly. Defaults to `1`""")
    )
    level: StrictStr = Field(
        default="INFO",
        description=format_docstring("""Controls the logging level to record for
        breadcrumbs. The Sentry Python SDK will record log records with a level
        higher than or equal to level as breadcrumbs. Inversely, the SDK
        completely ignores any log record with a level lower than this one.
        Defaults to 'INFO'""")
    )
    event_level: StrictStr = Field(
        default="ERROR",
        description=format_docstring("""Controls the logging level to record for
        events. The Sentry Python SDK will report log records with a level higher
        than or equal to event_level as events. Defaults to 'ERROR'""")
    )
    ignore_loggers: List[str] = Field(
        default_factory=list,
        description=format_docstring("""Loggers that should be ignored by the
        SDK. Ignored loggers do not send breadcrumbs or events. Defaults to an
        empty list (no loggers are ignored)""")
    )
    send_default_pii: bool = Field(
        default=False,
        description=format_docstring("""If this flag is enabled, certain
        personally identifiable information (PII) is added by active integrations.
        Defaults to `False` (No information is sent).""")
    )
    environment: str = Field(
        default="production",
        description=format_docstring("""Sets the environment. This string is
        freeform. A release can be associated with more than one environment to
        separate them in the UI. Defaults to 'production'.""")
    )
    traces_sample_rate: confloat(ge=0, le=1) = Field(
        default=1.0,
        description=format_docstring("""A number between 0 and 1, controlling
        the percentage chance a given transaction will be sent to Sentry.
        (0 represents 0% while 1 represents 100%.) Applies equally to all
        transactions created in the app. Defaults to `1`""")
    )
    max_breadcrumbs: int = Field(
        default=100,
        description=format_docstring("""Controls the total amount of breadcrumbs
        that should be captured. You can set this to any number. However, you
        should be aware that Sentry has a maximum
        [payload size](https://develop.sentry.dev/sdk/envelopes/#size-limits)
        and any events exceeding that payload size will be dropped. Defaults to
        `100`""")
    )

    class Config:
        env_file=".env"
        env_prefix="sentry_"

    def configure_sentry(self) -> None:
        """Configure sentry SDK from settings."""
        try:
            import sentry_sdk
        except ImportError:
            return
        if not self.enabled or not self.dsn:
            return
        
        integrations = []
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
        from sentry_sdk.integrations.pymongo import PyMongoIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        
        integrations.extend(
            [
                FastApiIntegration(),
                LoggingIntegration(
                    level=cast_logging_level(self.level),
                    event_level=cast_logging_level(self.event_level)
                ),
                PyMongoIntegration(),
                StarletteIntegration()
            ]
        )
        
        sentry_sdk.init(
            dsn=self.dsn,
            traces_sample_rate=self.traces_sample_rate,
            integrations=integrations,
            send_default_pii=self.send_default_pii,
            sample_rate=self.sample_rate,
            environment=self.environment,
            max_breadcrumbs=self.max_breadcrumbs
        )

        for logger in self.ignore_loggers:
            ignore_logger(logger)


class LoggingSettings(BaseSettings):
    config_path: FilePath | None = Field(
        default=pathlib.Path(__file__).parent.joinpath("./logging/logging.yml"),
        description=format_docstring("""The path to logging configuration file.
        This must be a .yml file. Defaults to '{}'""".format(
            pathlib.Path(__file__).parent.joinpath("./logging/logging.yml")
        ))
    )

    class Config:
        env_file=".env"
        env_prefix="logging_"
    
    def configure_logging(self) -> None:
        """Configure logging from a config file."""
        path = pathlib.Path(self.config_path)
        if not path.exists():
            raise FileNotFoundError(str(path))
        config = yaml.safe_load(path.read_text())
        logging.config.dictConfig(config)


class CachingSettings(BaseSettings):
    ttl: conint(gt=0) = Field(
        default=86_400,
        description=format_docstring("""The time to live (in seconds) for cached
        return values of memoized function caches. Defaults to `86400` (1 day)""")
    )

    class Config:
        env_file=".env"
        env_prefix="caching_"


class EventSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstring("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default=EventWorker.default_collection_name(),
        description=format_docstring("""The MongoDB collection to store
        events. Defaults to '{}'""".format(EventWorker.default_collection_name()))
    )

    class Config:
        env_file=".env"
        env_prefix="events_"


class TopicSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstring("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default="topics",
        description=format_docstring("""The MongoDB collection to store
        topics. Defaults to 'topics'""")
    )

    class Config:
        env_file=".env"
        env_prefix="topics_"


class EventBusSettings(BaseSettings):
    exchange: str = Field(
        default="hyprxa.exchange.events",
        description=format_docstring("""The exchange name to use for the event
        exchange. Defaults to 'hyprxa.exchange.events'""")
    )
    max_subscribers: conint(gt=0) = Field(
        default=_BM["max_subscribers"].default,
        description=format_docstring("""The maximum number of concurrent
        subscribers which can run by a single event manager instance. Defaults to `{}`
        """.format(_BM["max_subscribers"].default))
    )
    maxlen: conint(gt=0) = Field(
        default=_BM["maxlen"].default,
        description=format_docstring("""The maximum number of events that can
        buffered on a subscriber. If the buffer limit on a subscriber is
        reached, the oldest events will be evicted as new events are added.
        Defaults to `{}`""".format(_BM["maxlen"].default))
    )
    max_buffered_events: conint(gt=0) = Field(
        default=_EM["max_buffered_events"].default,
        description=format_docstring("""The maximum number of events that can
        be buffered on the event manager. If the limit is reached, the bus will refuse
        to enqueue any published events. Defaults to `{}""".format(_EM["max_buffered_events"].default))
    )
    subscription_timeout: confloat(gt=0) = Field(
        default=_BM["subscription_timeout"].default,
        description=format_docstring("""The time to wait (in seconds) for the
        manager to be ready before rejecting the subscription request. Defaults
        to `{}`.""".format(_BM["subscription_timeout"].default))
    )
    reconnect_timeout: confloat(gt=0) = Field(
        default=_BM["reconnect_timeout"].default,
        description=format_docstring("""The time to wait (in seconds) for the
        manager to be ready before dropping an already connected subscriber.
        Defaults to `{}`""".format(_BM["reconnect_timeout"].default))
    )
    max_backoff: confloat(gt=0) = Field(
        default=_BM["max_backoff"].default,
        description=format_docstring("""The maximum backoff time (in seconds) to
        wait before trying to reconnect to RabbitMQ. Defaults to `{}`
        """.format(_BM["max_backoff"].default))
    )
    initial_backoff: confloat(gt=0) = Field(
        default=_BM["initial_backoff"].default,
        description=format_docstring("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to RabbitMQ. Defaults to `{}`
        """.format(_BM["initial_backoff"].default))
    )

    async def get_manager(self) -> EventManager:
        """Configure and start an event manager instance from settings."""
        storage = EVENT_HANDLER_SETTINGS.get_handler()
        factory = RABBITMQ_SETTINGS.get_factory()
        bus = EventManager(
            storage=storage,
            factory=factory,
            exchange=self.exchange,
            max_buffered_events=self.max_buffered_events,
            max_subscribers=self.max_subscribers,
            maxlen=self.maxlen,
            subscription_timeout=self.subscription_timeout,
            reconnect_timeout=self.reconnect_timeout,
            max_backoff=self.max_backoff,
            initial_backoff=self.initial_backoff
        )
        await bus.start()
        return bus

    class Config:
        env_file=".env"
        env_prefix="events_"


class EventHandlerSettings(BaseSettings):
    flush_interval: confloat(gt=0) = Field(
        default=_MW["flush_interval"].default,
        description=format_docstring("""The time interval (in seconds) between
        document flushes to the database. Defaults to `{}`""".format(_MW["flush_interval"].default))
    )
    buffer_size: conint(gt=0) = Field(
        default=_MW["buffer_size"].default,
        description=format_docstring("""The maximum number of documents that
        can be buffered before flushing to the database. Defaults to `{}`
        """.format(_MW["buffer_size"].default))
    )
    max_retries: conint(gt=0) = Field(
        default=_MW["max_retries"].default,
        description=format_docstring("""The maximum number of attempts to flush
        pending documents before removing the pending documents. Defaults to `{}`
        """.format(_MW["max_retries"].default))
    )

    def get_handler(self) -> MongoEventHandler:
        """Configure an event handler instance from settings."""
        return MongoEventHandler(
            connection_uri=MONGO_SETTINGS.connection_uri,
            database_name=EVENT_SETTINGS.database_name,
            collection_name=EVENT_SETTINGS.collection_name,
            flush_interval=self.flush_interval,
            buffer_size=self.buffer_size,
            max_retries=self.max_retries,
            heartbeatFrequencyMS=MONGO_SETTINGS.heartbeat,
            serverSelectionTimeoutMS=MONGO_SETTINGS.server_selection_timeout,
            connectTimeoutMS=MONGO_SETTINGS.connect_timeout,
            socketTimeoutMS=MONGO_SETTINGS.socket_timeout,
            timeoutMS=MONGO_SETTINGS.timeout,
            appname=MONGO_SETTINGS.appname
        )
    class Config:
        env_file=".env"
        env_prefix="events_"


class TimeseriesSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstring("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default=TimeseriesWorker.default_collection_name(),
        description=format_docstring("""The MongoDB collection to store timeseries
        data. Defaults to '{}'""".format(TimeseriesWorker.default_collection_name()))
    )

    class Config:
        env_file=".env"
        env_prefix="timeseries_"


class UnitOpSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstring("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default="unitops",
        description=format_docstring("""The MongoDB collection to store unitop
        data. Defaults to 'unitops'""")
    )

    class Config:
        env_file=".env"
        env_prefix="unitops_"


class TimeseriesManagerSettings(BaseSettings):
    lock_ttl: conint(gt=0, le=15) = Field(
        default=_LK["ttl"].default,
        description=format_docstring("""The time (in seconds) to acquire and
        extend subscription locks for. Defaults to `{}`""".format(_LK["ttl"].default))
    )
    exchange: str = Field(
        default="hyprxa.exchange.timeseries",
        description=format_docstring("""The exchange name to use for the timeseries
        data exchange. Defaults to 'hyprxa.exchange.timeseries'""")
    )
    max_subscribers: conint(gt=0) = Field(
        default=_BM["max_subscribers"].default,
        description=format_docstring("""The maximum number of concurrent
        subscribers which can run by a single event manager instance. Defaults to `{}`
        """.format(_BM["max_subscribers"].default))
    )
    maxlen: conint(gt=0) = Field(
        default=_BM["maxlen"].default,
        description=format_docstring("""The maximum number of messages that can
        buffered on a subscriber. If the buffer limit on a subscriber is
        reached, the oldest messages will be evicted as new messages are added.
        Defaults to `{}`""".format(_BM["maxlen"].default))
    )
    max_buffered_messages: conint(gt=0) = Field(
        default=_TM["max_buffered_messages"].default,
        description=format_docstring("""The maximum number of messages that can
        be buffered on the managers storage queue. If the limit is reached, the
        manager will stop processing messages from the integration until the buffer
        drains. Defaults to `{}""".format(_TM["max_buffered_messages"].default))
    )
    subscription_timeout: confloat(gt=0) = Field(
        default=_BM["subscription_timeout"].default,
        description=format_docstring("""The time to wait (in seconds) for the
        manager to be ready before rejecting the subscription request. Defaults
        to `{}`.""".format(_BM["subscription_timeout"].default))
    )
    reconnect_timeout: confloat(gt=0) = Field(
        default=_BM["reconnect_timeout"].default,
        description=format_docstring("""The time to wait (in seconds) for the
        manager to be ready before dropping an already connected subscriber.
        Defaults to `{}`""".format(_BM["reconnect_timeout"].default))
    )
    max_backoff: confloat(gt=0) = Field(
        default=_BM["max_backoff"].default,
        description=format_docstring("""The maximum backoff time (in seconds) to
        wait before trying to reconnect to RabbitMQ. Defaults to `{}`
        """.format(_BM["max_backoff"].default))
    )
    initial_backoff: confloat(gt=0) = Field(
        default=_BM["initial_backoff"].default,
        description=format_docstring("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to RabbitMQ. Defaults to `{}`
        """.format(_BM["initial_backoff"].default))
    )
    max_failed: conint(gt=0) = Field(
        default=_TM["max_failed"].default,
        description=format_docstring("""The maximum number of failed connection
        attempts to RabbitMQ before the manager unsubscribes from all integration
        subscriptions. Defaults to `{}`""".format(_TM["max_failed"].default))
    )

    async def get_manager(self, source: Source) -> TimeseriesManager:
        """Configure and start a manager instance from settings."""
        storage = TIMESERIES_HANDLER_SETTINGS.get_handler()
        factory = RABBITMQ_SETTINGS.get_factory()
        memcached = MEMCACHED_SETTINGS.get_client()
        lock = SubscriptionLock(
            memcached=memcached,
            ttl=self.lock_ttl,
            max_workers=MEMCACHED_SETTINGS.max_pool_size
        )
        manager = TimeseriesManager(
            source=source,
            lock=lock,
            storage=storage,
            factory=factory,
            exchange=self.exchange,
            max_buffered_messages=self.max_buffered_messages,
            max_subscribers=self.max_subscribers,
            maxlen=self.maxlen,
            subscription_timeout=self.subscription_timeout,
            reconnect_timeout=self.reconnect_timeout,
            max_backoff=self.max_backoff,
            initial_backoff=self.initial_backoff,
            max_failed=self.max_failed
        )
        await manager.start()
        return manager

    class Config:
        env_file=".env"
        env_prefix="timeseries_"


class TimeseriesHandlerSettings(BaseSettings):
    flush_interval: confloat(gt=0) = Field(
        default=_MW["flush_interval"].default,
        description=format_docstring("""The time interval (in seconds) between
        document flushes to the database. Defaults to `{}`""".format(_MW["flush_interval"].default))
    )
    buffer_size: conint(gt=0) = Field(
        default=_MW["buffer_size"].default,
        description=format_docstring("""The maximum number of documents that
        can be buffered before flushing to the database. Defaults to `{}`
        """.format(_MW["buffer_size"].default))
    )
    max_retries: conint(gt=0) = Field(
        default=_MW["max_retries"].default,
        description=format_docstring("""The maximum number of attempts to flush
        pending documents before removing the pending documents. Defaults to `{}`
        """.format(_MW["max_retries"].default))
    )
    expire_after: conint(gt=0) = Field(
        default=_TH["expire_after"].default,
        description=format_docstring("""The time (in seconds) to persist timeseries
        documents in the database. For more information see
        [TTL Indexes](https://www.mongodb.com/docs/manual/core/index-ttl/).
        Defaults to `{}` seconds ({} days)""".format(
            _TH["expire_after"].default, _TH["expire_after"].default//86400
        ))
    )

    def get_handler(self) -> MongoTimeseriesHandler:
        """Configure a timeseries handler instance from settings."""
        return MongoTimeseriesHandler(
            connection_uri=MONGO_SETTINGS.connection_uri,
            database_name=TIMESERIES_SETTINGS.database_name,
            collection_name=TIMESERIES_SETTINGS.collection_name,
            flush_interval=self.flush_interval,
            buffer_size=self.buffer_size,
            max_retries=self.max_retries,
            expire_after=self.expire_after,
            heartbeatFrequencyMS=MONGO_SETTINGS.heartbeat,
            serverSelectionTimeoutMS=MONGO_SETTINGS.server_selection_timeout,
            connectTimeoutMS=MONGO_SETTINGS.connect_timeout,
            socketTimeoutMS=MONGO_SETTINGS.socket_timeout,
            timeoutMS=MONGO_SETTINGS.timeout,
            appname=MONGO_SETTINGS.appname
        )

    class Config:
        env_file=".env"
        env_prefix="timeseries_"


HYPRXA_SETTINGS = HyprxaSettings()
MONGO_SETTINGS = MongoSettings()
RABBITMQ_SETTINGS = RabbitMQSettings()
MEMCACHED_SETTINGS = MemcachedSettings()
SENTRY_SETTINGS = SentrySettings()
LOGGING_SETTINGS = LoggingSettings()
CACHE_SETTINGS = CachingSettings()
EVENT_SETTINGS = EventSettings()
EVENT_BUS_SETTINGS = EventBusSettings()
EVENT_HANDLER_SETTINGS = EventHandlerSettings()
TIMESERIES_SETTINGS = TimeseriesSettings()
TIMESERIES_MANAGER_SETTINGS = TimeseriesManagerSettings()
TIMESERIES_HANDLER_SETTINGS = TimeseriesHandlerSettings()
TOPIC_SETTINGS = TopicSettings()
UNITOP_SETTINGS = UnitOpSettings()