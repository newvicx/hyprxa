from typing import List

from hyprxa.base.manager import BaseManager
from hyprxa.caching.singleton import singleton
from hyprxa.events.models import EventManagerInfo
from hyprxa.timeseries.models import TimeseriesManagerInfo
from hyprxa.util.models import BaseModel



class Info(BaseModel):
    """Model for API statistics."""
    status: str
    info: List[EventManagerInfo | TimeseriesManagerInfo | None]


async def get_info() -> Info:
    """"Return diagnostic information about managers."""
    info = [obj.info for obj in singleton if isinstance(obj, BaseManager)]
    return Info(
        status="ok",
        info=info
    )