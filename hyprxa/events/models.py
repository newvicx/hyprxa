from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Tuple

import orjson
import pydantic
from jsonschema import SchemaError
from jsonschema.validators import validator_for
from pydantic import Field, root_validator, validator

from hyprxa.base import BaseSubscription, BaseSubscriptionRequest, BrokerInfo
from hyprxa.context import get_username
from hyprxa.util.models import BaseModel



def set_routing_key(v: Dict[str, str | None]) -> Dict[str, str]:
    """Set the routing key for the event."""
    topic = v.get("topic")
    routing_key = v.get("routing_key")
    if routing_key and routing_key.startswith(topic):
        return v
    if routing_key:
        routing_key = ".".join([split for split in routing_key.split(".") if split])
        v["routing_key"] = f"{topic}.{routing_key}"
    else:
        v["routing_key"] = f"{topic}.#"
    return v


class Topic(BaseModel):
    """A topic model tied to a class of events."""
    topic: str
    schema_: Dict[str, Any] = Field(alias="schema")
    
    @validator("schema_")
    def _is_valid_schema(cls, schema: Dict[str, Any]) -> Dict[str, Any]:
        validator_ = validator_for(schema)
        try:
            validator_.check_schema(schema)
        except SchemaError as e:
            raise ValueError(f"{e.json_path}-{e.message}")
        return schema


@dataclass
class TopicDocument:
    """MongoDB document model for a topic."""
    topic: str
    schema: Dict[str, Any]
    modified_by: str | None
    modified_at: datetime


ValidatedTopicDocument = pydantic.dataclasses.dataclass(TopicDocument)


class TopicQueryResult(BaseModel):
    """Result set of topic query."""
    items: List[str | None] = field(default_factory=list)


class TopicSubscription(BaseSubscription):
    """Subscription model for a topic or subset of a topic."""
    topic: str
    routing_key: str | None
    
    @root_validator
    def _set_routing_key(cls, v: Dict[str, str | None]) -> Dict[str, str]:
        return set_routing_key(v)


class TopicSubscriptionRequest(BaseSubscriptionRequest):
    """Model to subscribe to multiple topics."""
    subscriptions: Sequence[TopicSubscription]


@dataclass(frozen=True)
class EventDocument:
    """MongoDB document model for an event."""
    topic: str
    routing_key: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    posted_by: str | None = field(default_factory=get_username)

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


ValidatedEventDocument = pydantic.dataclasses.dataclass(EventDocument)


class Event(BaseModel):
    """An event to publish."""
    topic: str
    routing_key: str | None
    payload: Dict[str, Any]

    @root_validator
    def _set_routing_key(cls, v: Dict[str, str | None]) -> Dict[str, str]:
        return set_routing_key(v)
    
    def publish(self) -> Tuple[str, bytes]:
        return self.routing_key, orjson.dumps(self.payload)

    def to_document(self) -> EventDocument:
        return EventDocument(
            topic=self.topic,
            routing_key=self.routing_key,
            payload=self.payload
        )


class EventQueryResult(BaseModel):
    """Result set of an event query."""
    items: List[EventDocument]


class EventBusInfo(BrokerInfo):
    """Model for event bus statistics."""
    publish_buffer_size: int
    storage_buffer_size: int
    total_published_events: int
    total_stored_events: int
    storage_info: Dict[str, Any]