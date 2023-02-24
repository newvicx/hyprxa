from datetime import datetime, timedelta

import dateutil.parser
import pendulum
from fastapi import HTTPException, Query, status
from starlette.requests import Request

from hyprxa.util.defaults import DEFAULT_TIMEZONE
from hyprxa.util.filestream import FileWriter, get_file_format_writer



def get_file_writer(request: Request) -> FileWriter:
    """Returns a buffer/writer/suffix/media type combo for streaming files.
    
    Defaults to csv writer if "accept" isnt present.
    """
    accept = request.headers.get("accept", "text/csv")
    try:
        return get_file_format_writer(accept)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)


def parse_timestamp(query: Query, default_timedelta: int | None = None):
    """Parse a `str` timestamp from a request.
    
    The timestamp is returned in UTC. The input str timezone is assumed to be
    the application timezone unless otherwise specified.
    """
    if default_timedelta is not None:
        default_timedelta = timedelta(seconds=default_timedelta)
    else:
        default_timedelta = timedelta(seconds=0)
    def wrapper(time: str | None = query, timezone: str = DEFAULT_TIMEZONE) -> datetime:
        timezone = timezone or DEFAULT_TIMEZONE
        
        now = pendulum.now(tz="UTC").replace(tzinfo=None)
        if not time:
            return now - default_timedelta
        try:
            return pendulum.instance(
                dateutil.parser.parse(time), tz=timezone
            ).in_timezone("UTC").replace(tzinfo=None)
        except dateutil.parser.ParserError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
    return wrapper