from enum import Enum, IntEnum
from datetime import datetime
from typing import List

from pydantic import BaseModel



class SubscriberCodes(IntEnum):
    """Codes returned from a `wait` on a subscriber."""
    STOPPED = 1
    DATA = 2


class BrokerStatus(str, Enum):
    """Status of RabbitMQ broker connection."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"


class SubscriberInfo(BaseModel):
    """Model for subscriber statistics."""
    name: str
    stopped: bool
    created: datetime
    uptime: int
    total_published_messages: int
    total_subscriptions: int


class BrokerInfo(BaseModel):
    """Model for manager statistics."""
    name: str
    closed: bool
    status: BrokerStatus
    created: datetime
    uptime: int
    active_subscribers: int
    active_subscriptions: int
    subscriber_capacity: int
    total_subscribers_serviced: int
    subscriber_info: List[SubscriberInfo | None]