from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Tuple

import orjson
import pydantic

from hyprxa.base.models import ManagerInfo
from hyprxa.util.context import get_user_identity
from hyprxa.util.models import BaseModel, StorageHandlerInfo



@dataclass
class EventDocument:
    """MongoDB document model for an event."""
    topic: str
    routing_key: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    posted_by: str | None = field(default_factory=get_user_identity)

    def publish(self) -> Tuple[str, bytes]:
        return self.routing_key, orjson.dumps(asdict(self))

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, EventDocument):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return self.timestamp > __o.timestamp
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, EventDocument):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return self.timestamp < __o.timestamp
        except TypeError:
            return False


class Event(BaseModel):
    """An event to publish."""
    topic: str
    routing_key: str
    payload: Dict[str, Any]

    def to_document(self) -> EventDocument:
        return EventDocument(
            topic=self.topic,
            routing_key=self.routing_key,
            payload=self.payload
        )


class EventQueryResult(BaseModel):
    """Result set of an event query."""
    items: List[EventDocument]


ValidatedEventDocument = pydantic.dataclasses.dataclass(EventDocument)


class EventManagerInfo(ManagerInfo):
    """Model for event manager statistics."""
    publish_buffer_size: int
    storage_buffer_size: int
    total_published: int
    total_stored: int
    storage: StorageHandlerInfo