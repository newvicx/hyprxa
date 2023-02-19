import hashlib
from collections.abc import Sequence
from datetime import datetime
from enum import Enum, IntEnum
from typing import List

import orjson
from pydantic import validator

from hyprxa.util.models import BaseModel



class BaseSubscription(BaseModel):
    """A hashable and sortable subscription model.
    
    Models must be json encode/decode(able). Hashes use the JSON string
    representation of the object and are consistent across runtimes.

    Hashing: The `dict()` representation of the model is converted to a JSON
    byte string which is then sorted. The hashing algorithm used is SHAKE 128
    with a 16 byte length. Finally, the hex digest is converted to a base 10
    integer.

    Note: Implementations must not override the comparison operators.
    These operators are based on the hash of the model which is critical when
    sorting sequences of mixed implementation types.
    """
    class Config:
        frozen=True

    def __hash__(self) -> int:
        try:
            o = bytes(sorted(orjson.dumps(self.dict())))
        except Exception as e:
            raise TypeError(f"unhashable type: {e.__str__()}")
        return int(hashlib.shake_128(o).hexdigest(16), 16)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            return False
        try:
            return hash(self) == hash(__o)
        except TypeError:
            return False
    
    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) > hash(__o)
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) < hash(__o)
        except TypeError:
            return False
        

class BaseSubscriptionRequest(BaseModel):
    """Base model for a sequence of subscriptions that a user registers with
    one or more integrations.
    """
    subscriptions: Sequence[BaseSubscription]
    
    @validator("subscriptions")
    def _sort_subscriptions(
        cls,
        subscriptions: Sequence[BaseSubscription]
    ) -> List[BaseSubscription]:
        subscriptions = set(subscriptions)
        return sorted(subscriptions)
        

class SubscriberCodes(IntEnum):
    """Codes returned from a `wait` on a subscriber."""
    STOPPED = 1
    DATA = 2


class ManagerStatus(str, Enum):
    """Status of manager connection to RabbitMQ."""
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


class ManagerInfo(BaseModel):
    """Model for manager statistics."""
    name: str
    closed: bool
    status: ManagerStatus
    created: datetime
    uptime: int
    active_subscribers: int
    active_subscriptions: int
    subscriber_capacity: int
    total_subscribers_serviced: int
    subscribers: List[SubscriberInfo | None]