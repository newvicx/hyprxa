import json
import logging
import queue
import sys
import traceback
import warnings
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from hyprxa.util.mongo import MongoWorker
from hyprxa.util.logging import cast_logging_level



class LogWorker(MongoWorker):
    """Manages the submission of logs to MongoDB in a background thread."""
    def __init__(self, **kwargs: Any) -> None:
        self._expire_after = kwargs.pop("expire_after")
        super().__init__(**kwargs)

    def default_collection_name(self) -> str:
        return "logs"

    def send(self, client: MongoClient, exiting: bool = False) -> None:
        done = False

        max_batch_size = self._buffer_size

        db = client[self._database_name]
        try:
            collection = Collection(db, self._collection_name)
            collection.create_index("timestamp", expireAfterSeconds=self._expire_after)
        except OperationFailure:
            warnings.warn(
                f"Attempted to set a different expiry for {self._collection_name} "
                "collection. An existing TTL index already exists and will be "
                "used instead.",
                stacklevel=2
            )
            collection = db[self._collection_name]

        # Loop until the queue is empty or we encounter an error
        while not done:
            try:
                while len(self._pending_documents) < max_batch_size:
                    document = self._queue.get_nowait()
                    self._pending_documents.append(document)
                    self._pending_size += sys.getsizeof(document)

            except queue.Empty:
                done = True

            if not self._pending_documents:
                continue

            try:
                collection.insert_many(self._pending_documents, ordered=False)
                self._pending_documents.clear()
                self._pending_size = 0
                self._retries = 0
            except Exception:
                # Attempt to send on the next call instead
                done = True
                self._retries += 1

                # Roughly replicate the behavior of the stdlib logger error handling
                if logging.raiseExceptions and sys.stderr:
                    sys.stderr.write("--- commandcenter logging error ---\n")
                    traceback.print_exc(file=sys.stderr)
                    sys.stderr.write(json.dumps(self.info, indent=2))
                    if exiting:
                        sys.stderr.write(
                            "The log worker is stopping and these logs will not be sent.\n"
                        )
                    elif self._retries > self._max_retries:
                        sys.stderr.write(
                            "The log worker has tried to send these logs "
                            f"{self._retries} times and will now drop them."
                        )
                    else:
                        sys.stderr.write(
                            "The log worker will attempt to send these logs again in "
                            f"{self._flush_interval}s\n"
                        )

                if self._retries > self._max_retries:
                    # Drop this batch
                    self._pending_documents.clear()
                    self._pending_size = 0
                    self._retries = 0


class MongoLogHandler(logging.Handler):
    """A logging handler that sends logs to MongoDB.

    Args:
        connection_url: Mongo DSN connection url.
        database_name: The database to save logs to.
        collection_name: The collection name to save logs to. Defaults to 'logs'.
        flush_interval: The time between flushes on the worker. Defaults to 10
            seconds.
        flush_level: The log level which will trigger an automatic flush of the
            pending logs. Defaults to `logging.ERROR`
        buffer_size: The number of logs that can be buffered before a flush
            is done to the database.
        max_retries: The maximum number of attempts to make sending a batch of
            logs before giving up on the batch. Defaults to 3
        expire_after: The value of the TTL index for logs. Defaults to having
            logs expire after 14 days.
    """
    worker: LogWorker = None

    def __init__(
        self,
        flush_level: int | str,
        expire_after: int = 2_592_000,
        **kwargs: Any
    ) -> None:
        kwargs.update({"expire_after": expire_after})
        self.kwargs = kwargs
        self._flush_level = cast_logging_level(flush_level)

    def start_worker(self) -> LogWorker:
        """Start the log worker thread."""
        worker = LogWorker(**self.kwargs)
        worker.start()
        worker.wait(timeout=2)
        if not worker.is_running:
            raise TimeoutError("Timed out waiting for worker.")
        return worker

    def get_worker(self) -> LogWorker:
        """Get a log worker. If a worker does not exist or the worker
        is not running, a new worker is started.
        """
        if self.worker is None:
            worker = self.start_worker()
            self.worker = worker
        elif not self.worker.is_running:
            worker, self.worker = self.worker, None
            if not worker.is_stopped:
                worker.stop()
            worker = self.start_worker()
            self.worker = worker
        return self.worker

    @classmethod
    def flush(cls, block: bool = False):
        """Tell the log worker to send any currently enqueued logs.
        
        Blocks until enqueued logs are sent if `block` is set.
        """
        if cls.worker is not None:
            cls.worker.flush(block)

    def emit(self, record: logging.LogRecord):
        """Send a log to the log worker."""
        try:
            self.get_worker().publish(self.format(record))
        except Exception:
            self.handleError(record)
        else:
            if record.levelno == self._flush_level:
                self.get_worker().flush()

    def close(self) -> None:
        """Shuts down this handler and the flushes the log worker."""
        if self.worker is not None:
            # Flush the worker instead of stopping because another instance may
            # be using it
            self.worker.flush()

        return super().close()