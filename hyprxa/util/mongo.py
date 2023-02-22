import atexit
import logging
import queue
import threading
from collections.abc import AsyncIterable, Awaitable
from contextlib import asynccontextmanager
from contextvars import Context
from typing import Any, AsyncContextManager, Callable, Dict, List

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    PyMongoError,
    WaitQueueTimeoutError
)

from hyprxa.util.defaults import DEFAULT_DATABASE
from hyprxa._exceptions import HyprxaError



_LOGGER = logging.getLogger("hyprxa.util")


class DatabaseUnavailable(HyprxaError):
    """Raised when we are unable to ping the MongoDB server."""


class MongoWorker:
    """Manages the submission of documents to MongoDB in a background thread."""
    def __init__(
        self,
        connection_uri: str = "mongodb://localhost:27017",
        database_name: str | None = None,
        collection_name: str | None = None,
        flush_interval: int = 10,
        buffer_size: int = 100,
        max_retries: int = 3,
        **kwargs: Any
    ) -> None:
        kwargs.pop("maxPoolSize", None)

        self._connection_uri = connection_uri
        self._database_name = database_name or DEFAULT_DATABASE
        self._collection_name = collection_name or self.default_collection_name()
        self._flush_interval = flush_interval
        self._buffer_size = buffer_size
        self._max_retries = max_retries
        self._connection_args = kwargs

        self._runner = threading.Thread(target=self._run, daemon=True)

        self._queue: queue.Queue[Dict[Any, Any]] = queue.Queue()
        self._lock = threading.Lock()
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._send_finished_event = threading.Event()
        self._running_event = threading.Event()
        self._started = False
        self._stopped = False

        # Tracks documents that have been pulled from the queue but not sent
        # successfully
        self._pending_documents: List[Dict[Any, Any]] = []
        self._pending_size: int = 0
        self._retries = 0

        atexit.register(self.stop)

    @property
    def info(self) -> Dict[str, Any]:
        """Returns debugging information with worker sample stats."""
        return {
            "running": self.is_running,
            "stopped": self.is_stopped,
            "queue_length": self._queue.qsize(),
            "pending_batch_length": len(self._pending_documents),
            "pending_batch_size": self._pending_size
        }

    @property
    def is_running(self) -> bool:
        """`True` if worker can process documents."""
        with self._lock:
            return self._running_event.is_set()
    
    @property
    def is_stopped(self) -> bool:
        """`True` if worker is stopped."""
        with self._lock:
            return self._stopped

    @staticmethod
    def default_collection_name() -> str:
        """Define a default collection for the worker."""
        raise NotImplementedError()

    def start(self) -> None:
        """Start the background thread."""
        with self._lock:
            if not self._started and not self._stopped:
                Context().run(self._runner.start)
                self._started = True
            elif self._stopped:
                raise RuntimeError(
                    "The log worker cannot be started after stopping."
                )

    def stop(self) -> None:
        """Flush all documents and stop the background thread."""
        with self._lock:
            if self._started:
                self._flush_event.set()
                self._stop_event.set()
                self._runner.join()
                self._started = False
                self._stopped = True

    def flush(self, block: bool = False) -> None:
        """Flush all documents to the database."""
        with self._lock:
            if not self._started and not self._stopped:
                raise RuntimeError("Worker was never started.")
            self._flush_event.set()
            if block:
                self._send_finished_event.wait(30)

    def publish(self, document: Dict[Any, Any]):
        """Enqueue a document for the worker to submit."""
        with self._lock:
            if self._stopped:
                raise RuntimeError(
                    "Samples cannot be enqueued after the worker is stopped."
                )
            self._queue.put_nowait(document)

    def send(self, client: MongoClient, exiting: bool = False) -> None:
        """Send all documents in the queue in batches to avoid network limits.

        If a client error is encountered, the samples pulled from the queue should
        be retained up to `max_retries` times.
        """
        raise NotImplementedError()

    def wait(self, timeout: float | None = None) -> None:
        """Wait for worker to establish connection to MongoDB."""
        self._running_event.wait(timeout)

    def _run(self):
        try:
            with MongoClient(
                self._connection_uri,
                maxPoolSize=1,
                **self._connection_args
            ) as client:
                pong = client.admin.command("ping")
                if not pong.get("ok"):
                    raise DatabaseUnavailable("Unable to ping server.")
                self._running_event.set()
                while not self._stop_event.is_set():
                    self._flush_event.wait(self._flush_interval)
                    self._flush_event.clear()
                    
                    self.send(client)

                    self._send_finished_event.set()
                    self._send_finished_event.clear()

                # After the stop event, we are exiting...
                # Try to send any remaining pending documents
                self.send(client, True)
        except Exception:
            _LOGGER.error("The worker encountered a fatal error", exc_info=True)
        finally:
            self._send_finished_event.set()
            self._running_event.clear()


class SessionManager:
    """Manages a MongoDB database client.
    
    When acquiring a client, if a client is already open, that client will be
    reused otherwise a new client is created.

    IMPORTANT: This session manager should only be used for read operations or
    atomic write operations.

    Args:
        factory: A callable that returns an `AsyncIOMotorClient`.
    
    Examples:
    >>> manager = SessionManager(...)
    >>> async def do_operation(m: SessionManager) -> None:
    ...     async with m.get_client() as client:
    ...         print(id(client))
    ...         ...
    
    These operations will use the same client instance...
    >>> await asyncio.gather(do_operation(manager), do_operation(manager))
    These operations will use a different client instance...
    >>> await do_operation(manager)
    >>> await do_operation(manager)
    
    The session manager is not intended to maintain a long lived client, it is
    intended to minimize the total number of database connections during periods
    of high load. If a incoming request comes in while another request is still
    processing both requests will use the same connection pool. The pool
    will close when all requests have completed.
    """

    def __init__(self, factory: Callable[[], Awaitable[AsyncIOMotorClient]]) -> None:
        self._factory = factory
        self._client: AsyncIOMotorClient = None
        self._count: int = 0
    
    @asynccontextmanager
    async def get_client(self) -> AsyncContextManager[AsyncIOMotorClient]:
        try:
            self._count += 1
            try:
                if self._client is None:
                    client = await self._factory()
                    self._client = client
                
                pong = await self._client.admin.command("ping")
                if not pong.get("ok"):
                    raise DatabaseUnavailable("Unable to ping server.")
                
                yield self._client
            except WaitQueueTimeoutError:
                _LOGGER.warning(
                    "Operation timed out waiting for connection. The service or"
                    "the database may be experiencing high load.",
                    exc_info=True
                )
                raise
            except AutoReconnect:
                _LOGGER.warning(
                    "There was a connection interruption which caused an operation",
                    "to fail",
                    exc_info=True
                )
                raise
            except (PyMongoError, DatabaseUnavailable):
                self._count = 0
                _LOGGER.warning("Error in database client", exc_info=True)
                raise
        finally:
            self._count -= 1
            if self._count <= 0:
                self._count = 0
                client, self._client = self._client, None
                if client is not None:
                    _LOGGER.info("Closing database client")
                    client.close()


async def watch_collection(collection: AsyncIOMotorCollection) -> AsyncIterable[Any]:
    """Open a change stream to a collection and stream changes."""
    pipeline = [{'$match': {'operationType': 'insert'}}]
    async with collection.watch(pipeline) as stream:
        async for change in stream:
            yield change