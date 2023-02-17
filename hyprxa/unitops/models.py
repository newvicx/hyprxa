from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import pydantic
from pydantic import Field

from hyprxa.timeseries.models import AnySourceSubscription
from hyprxa.util.models import BaseModel



class UnitOp(BaseModel):
    """Model for a unitop. A unitop is a logical grouping of timeseries
    subscriptions."""
    name: str
    data_mapping: Dict[str, AnySourceSubscription]
    meta: Dict[str, Any]


@dataclass
class UnitOpDocument:
    """Document model for a unitop."""
    name: str
    data_mapping: Dict[str, AnySourceSubscription]
    meta: Dict[str, Any]
    modified_by: str
    modified_at: datetime


class UnitOpQueryResult(BaseModel):
    """Result set of unitop query."""
    items: List[UnitOpDocument | None] = Field(default_factory=list)


ValidatedUnitOpDocument = pydantic.dataclasses.dataclass(UnitOpDocument)