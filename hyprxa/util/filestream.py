import csv
import functools
import io
import logging
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from typing import Any, Callable

import jsonlines
import ndjson
from accept_types import get_best_match



def ndjson_writer(buffer: io.StringIO) -> Callable[[Any], None]:
    """A writer for ndjson streaming."""
    writer = ndjson.writer(buffer, ensure_ascii=False)
    return functools.partial(writer.writerow)


def jsonlines_writer(buffer: io.StringIO) -> Callable[[Any], None]:
    """A writer for JSONlines streaming."""
    writer = jsonlines.Writer(buffer)
    return functools.partial(writer.write)


def csv_writer(buffer: io.StringIO) -> Callable[[Iterable], None]:
    """A writer for CSV streaming."""
    writer = csv.writer(buffer, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    return functools.partial(writer.writerow)


def get_file_format_writer(accept: str) -> "FileWriter":
    """Matches the accept header to a file format writer."""
    accept_types = [
        "text/csv",
        "application/jsonlines",
        "application/x-jsonlines",
        "application/x-ndjson"
    ]
    best_match = get_best_match(accept.lower(), accept_types)
    buffer = io.StringIO()
    
    match best_match:
        case "text/csv":
            writer = csv_writer(buffer)
            return FileWriter(buffer, writer, ".csv", "text/csv")
        case "application/jsonlines" | "application/x-jsonlines":
            writer = jsonlines_writer(buffer)
            return FileWriter(buffer, writer, ".jsonl", "application/x-jsonlines")
        case "application/x-ndjson":
            writer = ndjson_writer(buffer)
            return FileWriter(buffer, writer, ".ndjson", "application/x-ndjson")
        case _:
            raise ValueError()


@dataclass
class FileWriter:
    buffer: io.StringIO
    writer: Callable[[Any], None]
    suffix: str
    media_type: str


async def chunked_transfer(
    send: AsyncIterable[Any],
    buffer: io.BytesIO | io.StringIO,
    writer: Callable[[Any], None],
    formatter: Callable[[Any], Any] | None,
    logger: logging.Logger,
    chunk_size: int = 1000
) -> AsyncIterable[bytes | str]:
    """Stream rows of data in chunks.
    
    Each row is appended to the buffer up to `chunk_size` at which point a
    flush is triggered and the buffer is cleared.

    `None` is considered a break and will trigger a flush.

    Args:
        iterator: The object to iterate over.
        buffer: The buffer that will hold data.
        formatter: A callable that accepts the raw output from the iterator and
            formats it so it can be understood by the writer.
        witer: A callable that accepts data from the formatter and writes the
            data to the buffer.
        chunk_size: The max number of iterations before a flush is triggered.

    Raises:
        Exception: Any exception raised by the iterator, writer, or formatter.
    """
    count = 0

    if formatter is None:
        write = lambda data: writer(data)
    else:
        write = lambda data: writer(formatter(data))
    
    try:
        async for data in send:
            if data is None:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                buffer.seek(0)
                buffer.truncate(0)
                chunk_size = 0
                continue
            
            try:
                write(data)
            except Exception:
                logger.error("Unhandled error in writer", exc_info=True)
                raise
            
            count += 1
            if count >= chunk_size:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                buffer.seek(0)
                buffer.truncate(0)
                count = 0
        else:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            return
    except Exception:
        logger.error("Unhandled exception in send", exc_info=True)