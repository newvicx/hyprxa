from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import pydantic
from jsonschema import SchemaError
from jsonschema.validators import validator_for
from pydantic import Field, root_validator, validator

from hyprxa.base import BaseSubscription
from hyprxa.util.events import get_routing_key
from hyprxa.util.models import BaseModel



class Topic(BaseModel):
    """A topic model tied to a class of events."""
    topic: str
    jschema: Dict[str, Any]
    
    @validator("jschema")
    def _is_valid_schema(cls, jschema: Dict[str, Any]) -> Dict[str, Any]:
        validator_ = validator_for(jschema)
        try:
            validator_.check_schema(jschema)
        except SchemaError as e:
            raise ValueError(f"{e.absolute_path}-{e.message}")
        return jschema


@dataclass
class TopicDocument:
    """MongoDB document model for a topic."""
    topic: str
    jschema: Dict[str, Any]
    modified_by: str | None
    modified_at: datetime


ValidatedTopicDocument = pydantic.dataclasses.dataclass(TopicDocument)


class TopicQueryResult(BaseModel):
    """Result set of topic query."""
    items: List[str | None] = Field(default_factory=list)


class TopicSubscription(BaseSubscription):
    """Subscription model for a topic or subset of a topic."""
    topic: str
    routing_key: str | None
    
    @root_validator
    def _set_routing_key(cls, v: Dict[str, str | None]) -> Dict[str, str]:
        topic = v.get("topic")
        routing_key = v.get("routing_key")
        v["routing_key"] = get_routing_key(topic=topic, routing_key=routing_key)
        return v