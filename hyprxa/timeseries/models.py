from dataclasses import dataclass, field
from datetime import datetime
from typing import Any



@dataclass
class TimeseriesDocument:
    """Mongo document model for a timeseries sample."""
    subscription: int
    timestamp: datetime
    value: Any
    expire: datetime = field(default_factory=datetime.utcnow)
    source: str | None = None