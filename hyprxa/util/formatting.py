import re
from typing import Tuple

from hyprxa.types import JSONPrimitive, TimeseriesRow



_CAPITALIZE_PATTERN = re.compile('(?<!^)(?=[A-Z])')


def snake_to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))


def snake_to_lower_camel(string: str) -> str:
    """Covert snake case (arg_a) to lower camel case (argA)."""
    splits = string.split('_')
    if len(splits) == 1:
        return string
    return f"{splits[0]}{''.join(word.capitalize() for word in splits[1:])}"


def camel_to_snake(string: str) -> str:
    """Convert camel (ArgA) and lower camel (argB) to snake case (arg_a)"""
    return _CAPITALIZE_PATTERN.sub('_', string).lower()


def format_timeseries_rows(row: TimeseriesRow) -> Tuple[JSONPrimitive]:
    """Formats a timeseries row as an iterable which can be converted to a row
    for a file format.
    """
    return (row[0].isoformat(), *row[1])


def format_docstring(description: str) -> str:
    """Takes a docstring formatted string and converts it to a string with no
    line breaks or extra spaces.
    """
    return " ".join(segment.strip() for segment in description.splitlines())