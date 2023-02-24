import logging
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Dict

from pydantic import ValidationError

from hyprxa.base.models import SubscriberCodes
from hyprxa.base.subscriber import BaseSubscriber
from hyprxa.timeseries.models import BaseSourceSubscription, SubscriptionMessage



_LOGGER = logging.getLogger("hyprxa.timeseries.subscriber")


class TimeseriesSubscriber(BaseSubscriber):
    """Subscriber implementation for timeseries data."""
    async def __aiter__(self) -> AsyncIterable[str]:
        if self.stopped:
            return
        ref: Dict[BaseSourceSubscription, datetime] = {}
        while not self.stopped:
            if not self._data:
                code = await self.wait()
                if code is SubscriberCodes.STOPPED:
                    return
            # Pop messages from the data queue until there are no messages
            # left
            while True:
                try:
                    data = self._data.popleft()
                    msg = SubscriptionMessage.parse_raw(data)
                except IndexError:
                    # Empty queue
                    break
                except ValidationError:
                    _LOGGER.warning("Validation failed for subscription message", extra={"raw": data})
                    continue
                
                last = ref.get(msg.subscription)
                if last is None or last < msg.items[-1].timestamp:
                    yield msg.json()
                ref[msg.subscription] = msg.items[-1].timestamp