import logging
import logging.config
import pathlib
import threading
from enum import Enum
from typing import Callable, List, Type

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
    StrictStr,
    confloat,
    conint
)

from hyprxa.util.defaults import DEFAULT_APPNAME, DEFAULT_DATABASE
from hyprxa.util.formatting import format_docstrings
from hyprxa.util.logging import cast_logging_level



class HyprxaSettings(BaseSettings):
    debug: bool = Field(
        default=False,
        description=format_docstrings("""If `True`, all authentication succeeds
        with a admin rights, sets the FastAPI application to debug mode, and sets
        all loggers to DEBUG. Defaults to `False`""")
    )
    interactive_auth: bool = Field(
        default=False,
        description=format_docstrings("""If `True`, enables interactive
        authentication at '/docs' page. Defaults to `False`""")
    )

    class Config:
        env_file=".env"
        env_prefix="hyprxa"


class MongoSettings(BaseSettings):
    connection_uri: MongoDsn = Field(
        default="mongodb://localhost:27017/",
        description=format_docstrings("""The connection uri to the MongoDB server.
        Defaults to 'mongodb://localhost:27017/'""")
    )
    heartbeat: conint(gt=0) = Field(
        default=None,
        description=format_docstrings("""The number of milliseconds between
        periodic server checks, or `None` to accept the default frequency for
        `MongoClient`. Defaults to `None`""")
    )
    server_selection_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstrings("""Controls how long (in milliseconds) the
        driver will wait to find an available, appropriate server to carry out a
        database operation; while it is waiting, multiple server monitoring
        operations may be carried out, each controlled by `connect_timeout`.
        Defaults to `10000` (10 seconds).""")
    )
    connect_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstrings("""Controls how long (in milliseconds) the
        driver will wait during server monitoring when connecting a new socket
        to a server before concluding the server is unavailable. 0 means no
        timeout. Defaults to `10000` (10 seconds)""")
    )
    socket_timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstrings("""Controls how long (in milliseconds) the
        driver will wait for a response after sending an ordinary (non-monitoring)
        database operation before concluding that a network error has occurred.
        0 means no timeout. Defaults to `10000` (10 seconds)""")
    )
    timeout: conint(gt=0) = Field(
        default=10_000,
        description=format_docstrings("""Controls how long (in milliseconds)
        the driver will wait when executing an operation (including retry
        attempts) before raising a timeout error. Defaults to `10000`""")
    )
    max_pool_size: conint(gt=0, le=100) = Field(
        default=4,
        description=format_docstrings("""The maximum allowable number of concurrent
        connections to each connected server. Requests to a server will block if
        there are `max_pool_size` outstanding connections to the requested server.
        Can be 0, in which case there is no limit on the number of concurrent
        connections. Defaults to `4`""")
    )
    appname: str = Field(
        default=DEFAULT_APPNAME,
        description=format_docstrings("""The name of the application that created
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
        env_prefix="mongodb"


class RabbitMQSettings(BaseSettings):
    connection_uri: AmqpDsn = Field(
        default="amqp://guest:guest@localhost:5672/",
        description=format_docstrings("""The connection uri to the RabbitMQ server.
        Defaults to 'amqp://guest:guest@localhost:5672/'""")
    )

    class Config:
        env_file=".env"
        env_prefix="rabbitmq"

    def get_factory(self) -> Callable[[], Connection]:
        return lambda: Connection(self.connection_uri)


class MemcachedSettings(BaseSettings):
    connection_uri: str = Field(
        default="localhost:11211",
        description=format_docstrings("""A `host:port` string to Memcached server.
        Defaults to 'localhost:11211'""")
    )
    connect_timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstrings("""Controls how long (in seconds) the driver
        will wait to connect a new socket to a server. Defaults to `10`""")
    )
    timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstrings("""Controls how long (in seconds) the driver
        will wait for send or recv calls on the socket connected to memcached.
        Defaults to `10`""")
    )
    max_pool_size: conint(gt=0) = Field(
        default=4,
        description=format_docstrings("""The maximum number of concurrent connections
        to the memcached server per client. Defaults to `4`""")
    )
    pool_idle_timeout: confloat(gt=0) = Field(
        default=60,
        description=format_docstrings("""The time (in seconds) before an unused
        pool connection is discarded. Defaults to `60`""")
    )

    def get_client(self) -> Memcached:
        return Memcached(
            self.connection_uri,
            connect_timeout=self.connect_timeout,
            timeout=self.timeout,
            no_delay=False,
            max_pool_size=self.max_pool_size,
            pool_idle_timeout=self.pool_idle_timeout
        )
    
    def get_limiter(self) -> threading.Semaphore:
        return threading.Semaphore(self.max_pool_size)

    class Config:
        env_file=".env"
        env_prefix="memcached"


class SentrySettings(BaseSettings):
    enabled: bool = Field(
        default=False,
        description=format_docstrings("""Controls whether the SDK is enabled for
        this runtime. If enabled, `dsn` must be specified. Defaults to `False`""")
    )
    dsn: AnyHttpUrl | None = Field(
        default=None,
        description=("""Tells the SDK where to send the events. If this value
        is not provided, the SDK will not send any events. Defaults to `None`""")
    )
    sample_rate: confloat(ge=0, le=1) = Field(
        default=1,
        description=format_docstrings("""A number between 0 and 1, controlling
        the percentage chance a given event will be sent to Sentry (0 represents
        0% while 1 represents 100%). Events are picked randomly. Defaults to `1`""")
    )
    level: StrictStr = Field(
        default="info",
        description=format_docstrings("""Controls the logging level to record for
        breadcrumbs. The Sentry Python SDK will record log records with a level
        higher than or equal to level as breadcrumbs. Inversely, the SDK
        completely ignores any log record with a level lower than this one.
        Defaults to 'INFO'""")
    )
    event_level: StrictStr = Field(
        default="error",
        description=format_docstrings("""Controls the logging level to record for
        events. The Sentry Python SDK will report log records with a level higher
        than or equal to event_level as events. Defaults to 'ERROR'""")
    )
    ignore_loggers: List[str] = Field(
        default_factory=list,
        description=format_docstrings("""Loggers that should be ignored by the
        SDK. Ignored loggers do not send breadcrumbs or events. Defaults to an
        empty list (no loggers are ignored)""")
    )
    send_default_pii: bool = Field(
        default=False,
        description=format_docstrings("""If this flag is enabled, certain
        personally identifiable information (PII) is added by active integrations.
        Defaults to `False` (No information is sent).""")
    )
    environment: str = Field(
        default="production",
        description=format_docstrings("""Sets the environment. This string is
        freeform. A release can be associated with more than one environment to
        separate them in the UI. Defaults to 'production'.""")
    )
    traces_sample_rate: confloat(ge=0, le=1) = Field(
        default=1.0,
        description=format_docstrings("""A number between 0 and 1, controlling
        the percentage chance a given transaction will be sent to Sentry.
        (0 represents 0% while 1 represents 100%.) Applies equally to all
        transactions created in the app. Defaults to `1`""")
    )
    max_breadcrumbs: int = Field(
        default=100,
        description=format_docstrings("""Controls the total amount of breadcrumbs
        that should be captured. You can set this to any number. However, you
        should be aware that Sentry has a maximum
        [payload size](https://develop.sentry.dev/sdk/envelopes/#size-limits)
        and any events exceeding that payload size will be dropped. Defaults to
        `100`""")
    )

    class Config:
        env_file=".env"
        env_prefix="sentry"

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
        description=format_docstrings("""The path to logging configuration file.
        This must be a .yml file. Defaults to '{}'""".format(
            pathlib.Path(__file__).parent.joinpath("./logging/logging.yml")
        ))
    )

    class Config:
        env_file=".env"
        env_prefix="logging"
    
    def configure_logging(self) -> None:
        """Configure logging from a config file."""
        # If the user has specified a logging path and it exists we will ignore the
        # default entirely rather than dealing with complex merging
        path = pathlib.Path(self.config_path)
        if not path.exists():
            raise FileNotFoundError(str(path))
        config = yaml.safe_load(path.read_text())
        logging.config.dictConfig(config)

        if hyprxa_settings.debug:
            loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
            for logger in loggers:
                logger.setLevel(level=logging.DEBUG)
            logging.getLogger().setLevel(level=logging.DEBUG)


class CachingSettings(BaseSettings):
    ttl: conint(gt=0) = Field(
        default=86_400,
        description=format_docstrings("""The time to live (in seconds) for cached
        return values of memoized function caches. Defaults to `86400` (1 day)""")
    )


class EventSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstrings("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default="events",
        description=format_docstrings("""The MongoDB collection to store
        events. Defaults to 'events'""")
    )

    class Config:
        env_file=".env"
        env_prefix="events"


class EventBusSettings(BaseSettings):
    exchange: str = Field(
        default="hyprxa.exchange.events",
        description=format_docstrings("""The exchange name to use for the event
        exchange. Defaults to 'hyprxa.exchange.events'""")
    )
    max_subscribers: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of concurrent
        subscribers which can run by a single event bus instance. Defaults to `100`""")
    )
    maxlen: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of events that can
        buffered on a subscriber. If the buffer limit on a subscriber is
        reached, the oldest events will be evicted as new events are added.
        Defaults to `100`""")
    )
    max_buffered_events: conint(gt=100) = Field(
        default=1000,
        description=format_docstrings("""The maximum number of events that can
        be buffered on the event bus. If the limit is reached, the bus will refuse
        to enqueue any published events. Defaults to `1000`""")
    )
    subscription_timeout: confloat(gt=0) = Field(
        default=5,
        description=format_docstrings("""The time to wait (in seconds) for the
        broker to be ready before rejecting the subscription request. Defaults
        to `5`.""")
    )
    reconnect_timeout: confloat(gt=0) = Field(
        default=60,
        description=format_docstrings("""The time to wait (in seconds) for the
        broker to be ready before dropping an already connected subscriber.
        Defaults to `60`""")
    )
    max_backoff: confloat(gt=0) = Field(
        default=3,
        description=format_docstrings("""The maximum backoff time (in seconds) to
        wait before trying to reconnect to the broker. Defaults to `3`""")
    )
    initial_backoff: confloat(gt=0) = Field(
        default=1,
        description=format_docstrings("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to the broker. Defaults to `1`""")
    )

    class Config:
        env_file=".env"
        env_prefix="events"


class EventHandlerSettings(BaseSettings):
    flush_interval: confloat(gt=0) = Field(
        default=10,
        description=format_docstrings("""The time interval (in seconds) between
        document flushes to the database. Defaults to `10`""")
    )
    buffer_size: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of documents that
        can be buffered before flushing to the database. Defaults to `100`""")
    )
    max_retries: conint(gt=0) = Field(
        default=3,
        description=format_docstrings("""The maximum number of attempts to flush
        pending documents before removing the pending documents. Defaults to `3`""")
    )

    class Config:
        env_file=".env"
        env_prefix="events"


class TimeseriesSettings(BaseSettings):
    database_name: str | None = Field(
        default=DEFAULT_DATABASE,
        description=format_docstrings("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default="timeseries",
        description=format_docstrings("""The MongoDB collection to store timeseries
        data. Defaults to 'timeseries'""")
    )
    sources: List[str] = Field(
        default_factory=list,
        description="""The available timeseries data sources users can subscribe
        to. Sources must be declared as an environment variable and the clients
        must be registered with the application in order to use the source.
        Defaults to an empty list (no sources to subscribe to)"""
    )

    def get_sources(self) -> Type[Enum]:
        """Generate an enum of source choices."""
        return Enum("Sources", {source.upper(): source.lower() for source in self.sources})

    class Config:
        env_file=".env"
        env_prefix="timeseries"


class TimeseriesManagerSettings(BaseSettings):
    lock_ttl: conint(gt=0) = Field(
        default=5,
        description=format_docstrings("""The time (in seconds) to acquire and
        extend subscription locks for. Defaults to `5`""")
    )
    exchange: str = Field(
        default="hyprxa.exchange.timeseries",
        description=format_docstrings("""The exchange name to use for the timeseries
        data exchange. Defaults to 'hyprxa.exchange.timeseries'""")
    )
    max_subscribers: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of concurrent
        subscribers which can run by a single manager instance. Defaults to `100`""")
    )
    maxlen: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of messages that can
        buffered on a subscriber. If the buffer limit on a subscriber is
        reached, the oldest messages will be evicted as new messages are added.
        Defaults to `100`""")
    )
    max_buffered_messages: conint(gt=100) = Field(
        default=1000,
        description=format_docstrings("""The maximum number of messages that can
        be buffered on the manager to be stored in the database. If the limit is
        reached, the manager will stop processing messages from clients until the
        buffer drains. Defaults to `1000`""")
    )
    subscription_timeout: confloat(gt=0) = Field(
        default=5,
        description=format_docstrings("""The time to wait (in seconds) for the
        broker to be ready before rejecting the subscription request. Defaults
        to `5`.""")
    )
    reconnect_timeout: confloat(gt=0) = Field(
        default=60,
        description=format_docstrings("""The time to wait (in seconds) for the
        broker to be ready before dropping an already connected subscriber.
        Defaults to `60`""")
    )
    max_backoff: confloat(gt=0) = Field(
        default=3,
        description=format_docstrings("""The maximum backoff time (in seconds) to
        wait before trying to reconnect to the broker. Defaults to `3`""")
    )
    initial_backoff: confloat(gt=0) = Field(
        default=1,
        description=format_docstrings("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to the broker. Defaults to `1`""")
    )
    max_failed: conint(gt=0) = Field(
        default=15,
        description=format_docstrings("""The maximum number of failed connection
        attempts to the broker before the manager unsubscribes from all client
        subscriptions. Defaults to `15`""")
    )

    class Config:
        env_file=".env"
        env_prefix="timeseries"


class TimeseriesHandlerSettings(BaseSettings):
    flush_interval: confloat(gt=0) = Field(
        default=10,
        description=format_docstrings("""The time interval (in seconds) between
        document flushes to the database. Defaults to `10`""")
    )
    buffer_size: conint(gt=0) = Field(
        default=100,
        description=format_docstrings("""The maximum number of documents that
        can be buffered before flushing to the database. Defaults to `100`""")
    )
    max_retries: conint(gt=0) = Field(
        default=3,
        description=format_docstrings("""The maximum number of attempts to flush
        pending documents before removing the pending documents. Defaults to `3`""")
    )
    expire_after: conint(gt=0) = Field(
        default=2_592_000,
        description=format_docstrings("""The time (in seconds) to persist timeseries
        documents in the database. For more information see
        [TTL Indexes](https://www.mongodb.com/docs/manual/core/index-ttl/).
        Defaults to `2_592_000` seconds (30 days)""")
    )

    class Config:
        env_file=".env"
        env_prefix="timeseries"


hyprxa_settings = HyprxaSettings()
mongo_settings = MongoSettings()
rabbitmq_settings = RabbitMQSettings()
memcached_settings = MemcachedSettings()
sentry_settings = SentrySettings()
logging_settings = LoggingSettings()
cache_settings = CachingSettings()
event_settings = EventSettings()
event_bus_settings = EventBusSettings()
event_handler_settings = EventHandlerSettings()
timeseries_settings = TimeseriesSettings()
timeseries_manager_settings = TimeseriesManagerSettings()
timeseries_handler_settings = TimeseriesHandlerSettings()