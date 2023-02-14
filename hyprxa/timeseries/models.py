from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Sequence, Set

from pydantic import Field, validator

from hyprxa.base import BaseSubscription, BrokerInfo
from hyprxa.util.models import BaseModel



class BaseSourceSubscription(BaseSubscription):
    """Base model for timeseries subscriptions."""
    source: str


class DroppedSubscriptions(BaseModel):
    """Message for dropped subscriptions from a client to a manager."""
    subscriptions: Set[BaseSourceSubscription | None]
    error: Exception | None

    class Config:
        arbitrary_types_allowed=True

    @validator("error")
    def _is_exception(cls, v: Exception) -> Exception:
        if v and not isinstance(v, Exception):
            raise TypeError(f"Expected 'Exception', got {type(v)}")
        return v


class BaseSourceSubscriptionRequest(BaseModel):
    """Base model for a sequence of subscriptions that a client registers with
    one or more sources.
    """
    subscriptions: Sequence[BaseSourceSubscription]
    
    @validator("subscriptions")
    def _sort_subscriptions(
        cls,
        subscriptions: Sequence[BaseSourceSubscription]
    ) -> List[BaseSourceSubscription]:
        subscriptions = set(subscriptions)
        return sorted(subscriptions)


class TimestampedValue(BaseModel):
    """Base model for any value with a timestamp."""
    timestamp: datetime = Field(default_factory=datetime.now)
    value: Any


@dataclass
class TimeseriesDocument:
    """MongoDB document model for a timeseries sample."""
    source: str
    subscription: int
    timestamp: datetime
    value: Any
    expire: datetime = field(default_factory=datetime.utcnow)

@dataclass
class TimeseriesSamples:
    source: str
    subscription: int
    items: List[TimestampedValue]

    def __iter__(self) -> Iterable[TimeseriesDocument]:
        for item in self.items:
            yield TimeseriesDocument(
                source=self.source,
                subscription=self.subscription,
                timestamp=item.timestamp,
                value=item.value
            )


class SubscriptionMessage(BaseModel):
    """Base model for any message emitted from a client.
    
    Messages must be json encode/decode(able).
    """
    subscription: BaseSourceSubscription
    items: List[TimestampedValue]

    def to_samples(self, source: str) -> TimeseriesSamples:
        return TimeseriesSamples(
            subscription=hash(self.subscription),
            items=self.items,
            source=source
        )


class AnySourceSubscription(BaseSourceSubscription):
    """Unconstrained subscription model to a data source."""
    class Config:
        extra="allow"


class AnySourceSubscriptionRequest(BaseSourceSubscriptionRequest):
    """Model for sequence of subscriptions to any number of data sources."""
    subscriptions: Sequence[AnySourceSubscription]

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
    source: str
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


class LockInfo(BaseModel):
    """Model for lock statistics."""
    name: str
    created: datetime
    uptime: int


class ManagerInfo(BrokerInfo):
    """Model for manager statistics."""
    client_info: ClientInfo
    lock_info: LockInfo
    total_published_messages: int
    total_stored_messages: int