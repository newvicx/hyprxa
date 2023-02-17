from typing import List

from hyprxa.base.broker import BaseBroker
from hyprxa.base.models import BrokerInfo
from hyprxa.caching.singleton import singleton
from hyprxa.util.models import BaseModel



class Info(BaseModel):
    status: str
    info: List[BrokerInfo | None]


async def get_info() -> Info:
    """"Return diagnostic information about brokers."""
    info = [obj.info for obj in singleton if isinstance(obj, BaseBroker)]
    return Info(
        status="ok",
        info=info
    )