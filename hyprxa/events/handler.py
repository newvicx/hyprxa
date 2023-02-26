import logging
import queue
import sys
import threading
from dataclasses import asdict
from datetime import datetime
from typing import Any

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection

from hyprxa.events.models import EventDocument
from hyprxa.util.models import StorageHandlerInfo, WorkerInfo
from hyprxa.util.mongo import MongoWorker



_LOGGER = logging.getLogger("hyprxa.events.db")


class EventWorker(MongoWorker):
    """Manages the submission of events to MongoDB in a background
    thread.
    """
    @staticmethod
    def default_collection_name() -> str:
        return "events"

    def send(self, client: MongoClient, exiting: bool = False) -> None:
        done = False

        max_batch_size = self._buffer_size

        db = client[self._database_name]
        collection = Collection(db, self._collection_name)
        collection.create_index(
            [
                ("timestamp", pymongo.ASCENDING),
                ("topic", pymongo.ASCENDING)
            ]
        )

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
                _LOGGER.debug("Sending batch of events to database", extra=self.info)
                for _ in range(len(self._pending_documents)):
                    document: EventDocument = self._pending_documents.pop(0)
                    collection.update_one(
                        filter={"topic": document.topic, "routing_key": document.routing_key, "count": {"$lt": 1000}},
                        update={
                            "$push": {
                                "events": asdict(document)
                            },
                            "$set": {
                                "timestamp": document.timestamp
                            },
                            "$inc": {"count": 1}
                        },
                        upsert=True
                    )
                else:
                    _LOGGER.debug("All events saved")
            except Exception:
                # Attempt to send on the next call instead
                done = True
                self._retries += 1

                _LOGGER.warning("Error in worker", exc_info=True)
                
                info = self.info
                
                if exiting:
                    _LOGGER.info("The worker is stopping", extra=info)
                elif self._retries > self._max_retries:
                    _LOGGER.error("Dropping events", extra=info)
                else:
                    _LOGGER.info("Resending events attempt %i of %i",
                        self._retries,
                        self._max_retries,
                        extra=info
                    )

                if self._retries > self._max_retries:
                    # Drop this batch of events
                    self._pending_documents.clear()
                    self._pending_size = 0
                    self._retries = 0


class MongoEventHandler:
    """A storage handler that sends events to MongoDB.

    The handler starts a worker thread in the background and submits events
    to the worker which flushes events to the database at set intervals or if
    the buffer reaches a certain size.

    If a worker fails, the next call to `publish` will attempt to start
    another worker for this handler. When a worker is started, the handler will
    wait up to 10 seconds and confirm if the worker is running. If it is not,
    the worker is stopped and `TimeoutError` is raised on `publish`. The caller
    can choose what to do with the events from there, including re-publish them.

    Args:
        connection_uri: Mongo DSN connection url.
        database_name: The database to save events to.
        collection_name: The collection name to save events to.
        flush_interval: The time (in seconds) between flushes on the worker.
        buffer_size: The number of events that can be buffered before a flush
            is done to the database.
        max_retries: The maximum number of attempts to make sending a batch of
            events before giving up on the batch.
        **kwargs: Additional kwargs for `MongoClient`.
    """
    worker: EventWorker = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self._lock = threading.Lock()

        self._created = datetime.utcnow()
        self._workers_used = 0

    @property
    def info(self) -> StorageHandlerInfo:
        worker = None
        if self.worker is not None:
            worker = WorkerInfo.parse_obj(self.worker.info)
        return StorageHandlerInfo(
            created=self._created,
            uptime=(datetime.utcnow() - self._created).total_seconds(),
            workers_used=self._workers_used,
            worker=worker
        )

    def start_worker(self) -> EventWorker:
        """Start the event worker thread."""
        worker = EventWorker(**self.kwargs)
        worker.start()
        worker.wait(10)
        if not worker.is_running:
            worker.stop()
            raise TimeoutError("Timed out waiting for worker thread to be ready.")
        self._workers_used += 1
        return worker

    def get_worker(self) -> EventWorker:
        """Get an event worker. If a worker does not exist or the worker
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

    def flush(self, block: bool = False):
        """Tell the worker to send any currently enqueued events.
        
        Blocks until enqueued events are sent if `block` is set.
        """
        with self._lock:
            if self.worker is not None:
                self.worker.flush(block)

    def publish(self, event: EventDocument):
        """Publish an event to the worker."""
        with self._lock:
            self.get_worker().publish(event)

    def close(self) -> None:
        """Shuts down this handler and the flushes the worker."""
        with self._lock:
            if self.worker is not None:
                self.worker.flush(block=True)
                self.worker.stop()