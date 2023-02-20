import logging
from collections.abc import Sequence
from functools import lru_cache
from typing import Any, Dict, Literal, Optional, Union

import flatten_dict

from hyprxa.util.logging import (
    json_dumps,
    merge_dicts,
    EXTRACTORS,
    LOGRECORD_DICT
)



class StandardFormatter(logging.Formatter):
    """Standard hyprxa formatter for the `logging` module.

    This formatter is based on the ECS logging format.
    
    Args:
        stack_trace_limit: Specifies the maximum number of frames to include for
            stack traces. Defaults to `None` which includes all available frames.
            Setting this to zero will suppress stack traces. This setting doesn't
            affect `LogRecord.stack_info` because this attribute is typically already
            pre-formatted.
        extra: Specifies the collection of meta-data fields to add to all records.
        exclude_fields: Specifies any fields that should be suppressed from the
            resulting fields, expressed with dot notation `exclude_keys=["error.stack_trace"]`
            You can also use field prefixes to exclude whole groups of fields
            `exclude_keys=["error"]`.
    """
    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        style: Union[Literal["%"], Literal["{"], Literal["$"]] = "%",
        validate: Optional[bool] = None,
        stack_trace_limit: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
        exclude_fields: Sequence[str] = (),
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)

        if stack_trace_limit is not None:
            if not isinstance(stack_trace_limit, int):
                raise TypeError(
                    "'stack_trace_limit' must be None, or a non-negative integer"
                )
            elif stack_trace_limit < 0:
                raise ValueError(
                    "'stack_trace_limit' must be None, or a non-negative integer"
                )

        if (
            not isinstance(exclude_fields, Sequence)
            or isinstance(exclude_fields, str)
            or any(not isinstance(item, str) for item in exclude_fields)
        ):
            raise TypeError("'exclude_fields' must be a sequence of strings")

        self._extra = extra
        self._exclude_fields = frozenset(exclude_fields)
        self._stack_trace_limit = stack_trace_limit

    def format(self, record: logging.LogRecord, asdict: bool = False) -> str:
        """Format `LogRecord` as ECS-like formatted JSON str"""
        result = self._prepare(record)
        return json_dumps(result)

    def _prepare(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Convert `LogRecord` to ECS-like formatted dict which can converted to a
        JSON string.
        """
        result = {}
        for field in EXTRACTORS.keys():
            if self._is_field_excluded(field):
                continue
            value = EXTRACTORS[field](record)
            if value is not None:
                merge_dicts(flatten_dict.unflatten({field: value}, splitter="dot"), result)
        
        available = record.__dict__

        # Pull all extras and flatten them to be sent into '_is_field_excluded'
        # since they can be defined as 'extras={"http": {"method": "GET"}}'
        extra_keys = set(available).difference(LOGRECORD_DICT)
        extras = flatten_dict.flatten({"extra": {key: available[key] for key in extra_keys}}, reducer="dot")

        # Merge in any keys that were set within 'extra={...}'
        for field, value in extras.items():
            if value is None:
                continue
            merge_dicts(flatten_dict.unflatten({field: value}, splitter="dot"), result)
        
        return result

    @lru_cache()
    def _is_field_excluded(self, field: str) -> bool:
        """`True` if `LogRecord` field is exclude field"""
        field_path = []
        for path in field.split("."):
            field_path.append(path)
            if ".".join(field_path) in self._exclude_fields:
                return True
        return False