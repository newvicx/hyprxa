import functools
import json
import logging
import sys
import traceback
from typing import Any, Dict, List, Type

import pendulum

from hyprxa.util.defaults import DEFAULT_TIMEZONE
from hyprxa.__version__ import __version__ as VERSION



def cast_logging_level(level: str | int) -> int:
    """Cast a logging level as str or int to int."""
    try:
        return int(level)
    except ValueError:
        match level.lower():
            case "notset":
                return 0
            case "debug":
                return 10
            case "info":
                return 20
            case "warning":
                return 30
            case "error":
                return 40
            case "critical":
                return 50
            case _:
                return 0


def merge_dicts(from_: Dict[str, Any], into: Dict[str, Any]) -> Dict[str, Any]:
    """Merge deeply nested dictionary structures."""
    for key, value in from_.items():
        into.setdefault(key, {})
        if isinstance(value, dict) and isinstance(into[key], dict):
            merge_dicts(value, into[key])
        elif into[key] != {}:
            raise TypeError(
                "Type mismatch at key `{}`: merging dicts would replace value "
                "`{}` with `{}`. This is likely due to dotted keys in the event "
                "dict being turned into nested dictionaries, causing a conflict.".format(
                    key, into[key], value
                )
            )
        else:
            into[key] = value
    return into


def record_attribute(attribute: str) -> Any:
    """Callable to obtain a LogRecord attribute at runtime."""
    return lambda r: getattr(r, attribute, None)


def record_error_type(record: logging.LogRecord) -> Type[Exception] | None:
    """Get the exc type from the traceback."""
    exc_info = record.exc_info
    if not exc_info:
        # exc_info is either an iterable or bool. If it doesn't
        # evaluate to True, then no error type is used.
        return None
    if isinstance(exc_info, bool):
        # if it is a bool, then look at sys.exc_info
        exc_info = sys.exc_info()
    if isinstance(exc_info, (list, tuple)) and exc_info[0] is not None:
        return exc_info[0].__name__
    return None


def record_error_message(record: logging.LogRecord) -> str | None:
    """Get the exc message from the traceback."""
    exc_info = record.exc_info
    if not exc_info:
        # exc_info is either an iterable or bool. If it doesn't
        # evaluate to True, then no error message is used.
        return None
    if isinstance(exc_info, bool):
        # if it is a bool, then look at sys.exc_info
        exc_info = sys.exc_info()
    if isinstance(exc_info, (list, tuple)) and exc_info[1]:
        return str(exc_info[1])
    return None


def record_error_stack_trace(record: logging.LogRecord) -> List[str] | None:
    """Obtain a formatted stack trace from the traceback."""
    # Using stack_info=True will add 'error.stack_trace' even
    # if the type is not 'error', exc_info=True only gathers
    # when there's an active exception.
    if record.exc_info and record.exc_info[2] is not None:
        return traceback.format_tb(record.exc_info[2]) or None
    # LogRecord only has 'stack_info' if it's passed via .log(..., stack_info=True)
    stack_info = getattr(record, "stack_info", None)
    if stack_info:
        return [str(stack_info)]
    return None


def json_dumps(value: Dict[str, Any]) -> str:
    # Ensure that the first 2 fields are 'timestamp' and 'message'
    ordered_fields = []
    try:
        ordered_fields.append(("timestamp", value.pop("timestamp")))
    except KeyError:
        pass

    json_dumps = functools.partial(
        json.dumps, sort_keys=True, separators=(",", ":"), default=json_dumps_fallback
    )

    # Because we want to use 'sorted_keys=True' we manually build
    # the first three keys and then build the rest with json.dumps()
    if ordered_fields:
        # Need to call json.dumps() on values just in
        # case the given values aren't strings (even though
        # they should be according to the spec)
        ordered_json = ",".join(f'"{k}":{json_dumps(v)}' for k, v in ordered_fields)
        if value:
            return "{{{},{}".format(
                ordered_json,
                json_dumps(value)[1:],
            )
        else:
            return "{%s}" % ordered_json
    # If there are no fields with ordering requirements we
    # pass everything into json.dumps()
    else:
        return json_dumps(value)


def json_dumps_fallback(value: Any) -> Any:
    """Fallback handler for json.dumps to handle objects json doesn't know how to
    serialize.
    """
    try:
        # This is what structlog's json fallback does
        return value.__structlog__()
    except AttributeError:
        return repr(value)


EXTRACTORS = {
    "timestamp": lambda r: pendulum.from_timestamp(
        r.created, tz=DEFAULT_TIMEZONE
    ).in_timezone("UTC").replace(tzinfo=None).isoformat(),
    "api_version": lambda _: VERSION,
    "context.correlation_id": record_attribute("correlation_id"),
    "context.user": record_attribute("user"),
    "context.ip_address": record_attribute("ip_address"),
    "context.host": record_attribute("host"),
    "log.level": lambda r: (r.levelname.lower() if r.levelname else None),
    "log.original": lambda r: r.getMessage(),
    "log.logger": record_attribute("name"),
    "origin.function": record_attribute("funcName"),
    "origin.file.line": record_attribute("lineno"),
    "origin.file.name": record_attribute("filename"),
    "process.pid": record_attribute("process"),
    "process.name": record_attribute("processName"),
    "process.thread.id": record_attribute("thread"),
    "process.thread.name": record_attribute("threadName"),
    "error.type": record_error_type,
    "error.message": record_error_message,
    "error.stack_trace": record_error_stack_trace,
}


# Load the attributes of a LogRecord so if some are
# added in the future we won't mistake them for 'extra=...'
try:
    LOGRECORD_DIR = set(dir(logging.LogRecord("", 0, "", 0, "", (), None)))
except Exception:  # LogRecord signature changed?
    LOGRECORD_DIR = set()


LOGRECORD_DICT = {
        "name",
        "msg",
        "args",
        "asctime",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
    } | LOGRECORD_DIR

# We need to add the record attributes that we added with filters and extracted
LOGRECORD_DICT.update({"user", "ip_address", "correlation_id", "host"})