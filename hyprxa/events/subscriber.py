from collections.abc import AsyncIterable

from hyprxa.base.models import SubscriberCodes
from hyprxa.base.subscriber import BaseSubscriber



class EventSubscriber(BaseSubscriber):
    """Subscriber implementation for events."""
    async def __aiter__(self) -> AsyncIterable[str]:
        if self.stopped:
            return
        while not self.stopped:
            if not self._data:
                code = await self.wait()
                if code is SubscriberCodes.STOPPED:
                    return
            # Pop messages from the data queue until there are no messages
            # left
            while True:
                try:
                    yield self._data.popleft()
                except IndexError:
                    # Empty queue
                    break