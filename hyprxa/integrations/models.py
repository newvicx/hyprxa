import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, IntEnum
from typing import Any, Dict, List, Sequence, Set

import orjson
from pydantic import BaseModel, Field, validator

from hyprxa.timeseries import TimeseriesDocument
from hyprxa.util.json import json_dumps, json_loads



class DroppedSubscriptions(BaseModel):
    """Message for dropped subscriptions from a client to a manager."""
    subscriptions: Set[Subscription | None]
    error: Exception | None

    class Config:
        arbitrary_types_allowed=True

    @validator("error")
    def _is_exception(cls, v: Exception) -> Exception:
        if v and not isinstance(v, Exception):
            raise TypeError(f"Expected 'Exception', got {type(v)}")
        return v


class SubscriptionRequest(BaseModel):
    """Base model for a sequence of subscriptions that a client registers with
    one or more integrations.
    """
    subscriptions: Sequence[Subscription]
    
    @validator("subscriptions")
    def _sort_subscriptions(cls, subscriptions: Sequence[Subscription]) -> List[Subscription]:
        subscriptions = set(subscriptions)
        return sorted(subscriptions)


class TimestampedValue(BaseModel):
    """Base model for any value with a timestamp."""
    timestamp: datetime = Field(default_factory=datetime.now)
    value: Any


class SubscriptionMessage(BaseModel):
    """Base model for any message emitted from a client.
    
    Messages must be json encode/decode(able).
    """
    subscription: Subscription
    items: List[TimestampedValue]

    class Config:
        json_dumps=json_dumps
        json_loads=json_loads

    def to_samples(self, source: str) -> "TimeseriesSamples":
        return TimeseriesSamples(
            subscription=hash(self.subscription),
            items=self.items,
            source=source
        )


@dataclass
class TimeseriesSamples:
    subscription: int
    items: List[TimestampedValue]
    source: str | None = None

    def __iter__(self) -> Iterable[TimeseriesDocument]:
        for item in self.items:
            yield TimeseriesDocument(
                subscription=self.subscription,
                timestamp=item.timestamp,
                value=item.value,
                source=self.source
            )


class BaseSourceSubscription(Subscription):
    """Base model for any subscription to a data source."""
    source: str


class BaseSourceSubscriptionRequest(SubscriptionRequest):
    """Base model for a sequence of subscriptions that a client registers with
    a data source.
    """
    subscriptions: Sequence[BaseSourceSubscription]


class AnySourceSubscription(BaseSourceSubscription):
    """Unconstrained subscription model to a data source."""
    class Config:
        extra="allow"


class AnySourceSubscriptionRequest(BaseSourceSubscriptionRequest):
    """Model for sequence of subscriptions to any number of data sources."""
    subscriptions: Sequence[BaseSourceSubscription]

    def group(self) -> Dict[str, List[AnySourceSubscription]]:
        """Group subscriptions together by source."""
        sources = set([subscription.source for subscription in self.subscriptions])
        groups = {}
        for source in sources:
            group = [
                subscription for subscription in self.subscriptions
                if subscription.source == source
            ]
            groups[source] = group
        return groups


class SubscriberCodes(IntEnum):
    """Codes returned from a `wait` on a subscriber."""
    STOPPED = 1
    DATA = 2


class BrokerStatus(str, Enum):
    """Status of RabbitMQ broker connection."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"


class ConnectionInfo(BaseModel):
    """Model for connection statistics."""
    name: str
    online: bool
    created: datetime
    uptime: int
    total_published_messages: int
    total_subscriptions: int


class ClientInfo(BaseModel):
    """Model for client statistics."""
    name: str
    closed: bool
    data_queue_size: int
    dropped_connection_queue_size: int
    created: datetime
    uptime: int
    active_connections: int
    active_subscriptions: int
    subscription_capacity: int
    total_connections_serviced: int
    connection_info: List[ConnectionInfo | None]


class SubscriberInfo(BaseModel):
    """Model for subscriber statistics."""
    name: str
    stopped: bool
    created: datetime
    uptime: int
    total_published_messages: int
    total_subscriptions: int


class LockInfo(BaseModel):
    """Model for lock statistics."""
    name: str
    backend: str
    created: datetime
    uptime: int


class ManagerInfo(BaseModel):
    """Model for manager statistics."""
    name: str
    closed: bool
    status: BrokerStatus
    created: datetime
    uptime: int
    lock: LockInfo
    active_subscribers: int
    active_subscriptions: int
    subscriber_capacity: int
    total_subscribers_serviced: int
    client_info: ClientInfo
    subscriber_info: List[SubscriberInfo | None]
    