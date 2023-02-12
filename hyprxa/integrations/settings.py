import inspect

from pydantic import BaseSettings, Field

from hyprxa.integrations.locks import Locks
from hyprxa.integrations.manager import ClientManager
from hyprxa.util.formatting import format_docstrings



defaults = inspect.signature(ClientManager).parameters


class ManagerSettings(BaseSettings):
    lock_backend: Locks = Field(
        default=Locks.REDIS.value,
        description=format_docstrings("""The locking backend to use for this
        the manager. Defaults to {}.""".format(Locks.REDIS.value))
    )
    exchange: str = Field(
        default=defaults["exchange"].default,
        description=format_docstrings("""The exchange name to use for the data
        exchange. Defaults to {}""".format(defaults["exchange"].default))
    )
    max_subscribers: int = Field(
        default=defaults["max_subscribers"].default,
        description=format_docstrings("""The maximum number of concurrent
        subscribers which can run by a single manager. Defaults to {}
        """.format(defaults["max_subscribers"].default))
    )
    maxlen: int = Field(
        default=defaults["maxlen"].default,
        description=format_docstrings("""The maximum number of messages that can
        buffered on the subscriber. If the buffer limit on the subscriber is
        reached, the oldest messages will be evicted as new messages are added.
        Defaults to {}""".format(defaults["maxlen"].default))
    )
    max_buffered_messages: int = Field(
        default=defaults["max_buffered_messages"].default,
        description=format_docstrings("""The maximum number of messages that can
        be buffered on the manager waiting to write to the database. Defaults to
        {}""".format(defaults["max_buffered_messages"].default))
    )
    subscription_timeout: float = Field(
        default=defaults["subscription_timeout"].default,
        description=format_docstrings("""The time to wait for the broker to be
        ready before rejecting the subscription request. Defaults to {}
        """.format(defaults["subscription_timeout"].default))
    )
    reconnect_timeout: float = Field(
        default=defaults["reconnect_timeout"].default,
        description=format_docstrings("""The time to wait for the broker to be
        ready before dropping an already connected subscriber. Defaults to {}
        """.format(defaults["reconnect_timeout"].default))
    )
    max_backoff: float = Field(
        default=defaults["max_backoff"].default,
        description=format_docstrings("""The maximum backoff time in seconds to
        wait before trying to reconnect to the broker. Defaults to {}
        """.format(defaults["max_backoff"].default))
    )
    initial_backoff: float = Field(
        default=defaults["initial_backoff"].default,
        description=format_docstrings("""The minimum amount of time in seconds
        to wait before trying to reconnect to the broker. Defaults to {}
        """.format(defaults["initial_backoff"].default))
    )
    max_failed: int = Field(
        default=defaults["max_failed"].default,
        description=format_docstrings("""The maximum number of failed connection
        attempts before all subscribers and client subscriptions tied to this
        manager are dropped. Defaults to {}""".format(defaults["max_failed"].default))
    )

    class Config:
        env_file=".env"
        env_prefix="hyprxa_manager"