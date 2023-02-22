import logging
from collections.abc import AsyncIterable, Generator
from typing import Any



async def sse_handler(send: AsyncIterable[Any], logger: logging.Logger) -> AsyncIterable[Any]:
    """Wraps an async iterable, yields events."""
    try:
        async for msg in send:
            yield msg
    except Exception:
        logger.error("Connection closed abnormally", exc_info=True)


class SSE:
    """Representation of an event from the event stream."""
    def __init__(
        self,
        id: int | None = None,
        event: str = 'message',
        data: str = '',
        retry: int | None = None
    ):
        self.id = id
        self.event = event
        self.data = data
        self.retry = retry

    def __str__(self):
        s = '{0} event'.format(self.event)
        if self.id:
            s += ' #{0}'.format(self.id)
        if self.data:
            s += ', {0} byte{1}'.format(len(self.data),
                                        's' if len(self.data) else '')
        else:
            s += ', no data'
        if self.retry:
            s += ', retry in {0}ms'.format(self.retry)
        return s


class SSEParser:
    """Sans-I/O sse parser."""
    def __init__(self, logger: logging.Logger, char_enc: str = "utf-8") -> None:
        self._logger = logger
        self._char_enc = char_enc
        
        self._buffer = b''

    def feed(self, chunk: bytes) -> None:
        for line in chunk.splitlines(True):
            self._buffer += line

    def events(self) -> Generator[SSE, None, None]:
        if self._buffer.endswith((b'\r\r', b'\n\n', b'\r\n\r\n')):
            event = SSE()
            # Split before decoding so splitlines() only uses \r and \n
            for line in self._buffer.splitlines():
                # Decode the line.
                line = line.decode(self._char_enc)

                # Lines starting with a separator are comments and are to be
                # ignored.
                if not line.strip() or line.startswith(":"):
                    continue

                data = line.split(":", 1)
                field = data[0]

                # Ignore unknown fields.
                if field not in event.__dict__:
                    self._logger.debug('Saw invalid field %s while parsing event', field)
                    continue

                if len(data) > 1:
                    # From the spec:
                    # "If value starts with a single U+0020 SPACE character,
                    # remove it from value."
                    if data[1].startswith(' '):
                        value = data[1][1:]
                    else:
                        value = data[1]
                else:
                    # If no value is present after the separator,
                    # assume an empty value.
                    value = ''

                # The data field may come over multiple lines and their values
                # are concatenated with each other.
                if field == 'data':
                    event.__dict__[field] += value + '\n'
                else:
                    event.__dict__[field] = value

            # Events with no data are not dispatched.
            if not event.data:
                return

            # If the data field ends with a newline, remove it.
            if event.data.endswith('\n'):
                event.data = event.data[0:-1]

            # Empty event names default to 'message'
            event.event = event.event or 'message'

            # Dispatch the event
            self._logger.debug('Dispatching %s...', event)
            yield event
            self._buffer = b''