import inspect

from pydantic import BaseSettings, Field

from hyprxa.timeseries.handler import MongoTimeseriesHandler, TimeseriesWorker
from hyprxa.util.defaults import DEFAULT_DATABASE



defaults = inspect.signature(MongoTimeseriesHandler).parameters


class TimeseriesSettings(BaseSettings):
    database_name: str | None = Field(
        default=defaults["database_name"].default,
        description="""The MongoDB database to connect to. Defaults to {}""".format(DEFAULT_DATABASE)
    )
    collection_name: str | None = Field(
        default=defaults["collection_name"].default,
        description="""The MongoDB collection to store timeseries data. Defaults
        to {}""".format(TimeseriesWorker.default_collection_name())
    )
    flush_interval: float = Field(
        default=defaults["flush_interval"].default,
        description="""Interval between document flushes to the database. Defaults
        to {}""".format(defaults["flush_interval"].default)
    )
    buffer_size: int = Field(
        default=defaults["buffer_size"].default,
        description="""The maximum number of documents that can be buffered before
        flushing to the database. Defaults to {}""".format(defaults["buffer_size"].default)
    )
    max_retries: int = Field(
        default=defaults["max_retries"].default,
        description="""The maximum number of attempts to flush pending documents
        before removing the pending documents. Defaults to {}""".format(defaults["max_retries"].default)
    )
    expire_after: int = Field(
        default=defaults["expire_after"].default,
        description="""The time (in seconds) to persist timeseries documents in
        the database. For more information see [TTL Indexes](https://www.mongodb.com/docs/manual/core/index-ttl/).
        Defaults to {} seconds ({} days)""".format(
            defaults["expire_after"].default,
            defaults["expire_after"].default//86400
        )
    )

    class Config:
        env_file=".env"
        env_prefix="cc_timeseries"