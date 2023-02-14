from typing import List

from fastapi import Depends

from hyprxa.auth import BaseUser
from hyprxa.base import BaseBroker, BrokerInfo
from hyprxa.caching import singleton
from hyprxa.dependencies.auth import is_admin
from hyprxa.util.models import BaseModel



class APIInfo(BaseModel):
    status: str
    broker_info: List[BrokerInfo | None]


async def debug_info(_: BaseUser = Depends(is_admin)) -> APIInfo:
    """"Return diagnostic information related to the API."""
    broker_info = [obj.info for obj in singleton if isinstance(obj, BaseBroker)]
    return APIInfo(
        status="ok",
        brokers=broker_info
    )