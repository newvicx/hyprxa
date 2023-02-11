import bisect
import logging
import threading
from collections.abc import Generator, Iterable, Sequence
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple, cast

from hyprxa.integrations import Subscription
from hyprxa.timeseries.local.chunks import Chunk, TimeChunk
from hyprxa.timeseries.local.timeseries import Timeseries
from hyprxa.types import TimeseriesRow



_LOGGER = logging.getLogger("hyprxa.timeseries")


class TimeseriesCollection:
    """A timeseries collection is an indexed container of timeseries objects.
    
    Timeseries are indexed by their subscription and can be accessed via the
    index operator ([]) similar to a mapping.
    
    You cannot mutate a timeseries collection while iterating over it.
    
    Timeseries collections are thread safe.
    """
    def __init__(self) -> None:
        self._series: Dict[int, Timeseries] = {}
        self._lock: threading.Lock = threading.Lock()

    def __getitem__(self, index: Subscription) -> Timeseries:
        return self._series[hash(index)]

    def create(
        self,
        subscription: Subscription,
        samples: Iterable[Tuple[datetime, Any]] = None,
        retention: int = 7200,
        chunk_size: int = 100,
        labels: Sequence[str] = None,
        overwrite: bool = False
    ) -> Timeseries:
        """Create a `Timeseries`.
        
        If the subscription already exists, this will return the existing timeseries
        object unless `overwrite=True` in which case the new object will replace
        the old.
        
        Args:
            subscription: The id of the timeseries.
            samples: Any initial samples that should be added to the timeseries. These
                must be sorted by timestamp in monotonically increasing order otherwise
                and `OldTimestampError` will be raised.
            retention: The retention period of the series (in seconds).
            chunk_size: The fixed length of chunks in the timeseries.
            labels: Meta data for querying timeseries in a collection.
            overwrite: If `True` overwrite the existing `Timeseries` if it already exists.
        
        Returns:
            timeseries: The `Timseries`.
        
        Raises:
            OldTimestampError: If the samples are provided but the timestamps
                are not sorted.
        """
        with self._lock:
            h = hash(subscription)
            if h in self._series and not overwrite:
                return self._series[h]
            series = Timeseries(subscription, samples, retention, chunk_size, labels)
            self._series[h] = series
            _LOGGER.debug("Created new timeseries: %s", subscription.json())
            return series

    def filter_by_subscription(
        self,
        subscriptions: Sequence[Subscription]
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.
        
        Args:
            subscriptions: A sequence of subscriptions to query.
        
        Returns:
            view: `TimeseriesCollectionView`
        """
        subscriptions: Set[Subscription] = set(subscriptions)

        def _filter() -> Iterable[Timeseries]:
            with self._lock:
                for series in self._series.values():
                    if series.subscription in subscriptions:
                        yield series
        
        series = _filter()
        return TimeseriesCollectionView(series)

    def filter_by_labels(
        self,
        labels: Sequence[str] = None,
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.
        
        Args:
            labels: A sequence of labels to query
        
        Returns:
            view: A `TimeseriesCollectionView`
        """
        labels: Set[str] = set(labels)

        def _filter() -> Iterable[Timeseries]:
            with self._lock:
                for series in self._series.values():
                    if labels.difference(series.labels) != labels:
                        yield series

        series = _filter()
        return TimeseriesCollectionView(series)

    def __aiter__(self) -> Iterable[TimeseriesRow]:
        subscriptions = [series.subscription for series in self._series.values()]
        view = self.filter_by_subscription(subscriptions)
        for row in view:
            yield row


class TimeseriesCollectionView(Iterable[TimeseriesRow]):
    """A collection view that is a one time use iterable for viewing a subset
    of a collection of timeseries.
    
    Views should never be created directly, they should only ever be created by
    a `TimeseriesCollection`.
    """
    def __init__(self, series: Iterable[Timeseries]) -> None:
        self._series = series

    def filter_by_subscription(
        self,
        subscriptions: Sequence[Subscription]
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.
        
        Args:
            subscriptions: A sequence of subscriptions to query.
        
        Returns:
            view: A `TimeseriesCollectionView`.
        """
        subscriptions: Set[Subscription] = set(subscriptions)

        def _filter() -> Iterable[Timeseries]:
            for series in self._series:
                if series.subscription in subscriptions:
                    yield series
        
        series = _filter()
        return TimeseriesCollectionView(series)

    def filter_by_labels(
        self,
        labels: Sequence[str] = None,
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.
        
        Args:
            labels: A sequence of labels to query.
        
        Returns:
            view: A `TimeseriesCollectionView`.
        """
        labels: Set[str] = set(labels)

        def _filter() -> Iterable[Timeseries]:
            for series in self._series:
                if labels.difference(series.labels) != labels:
                    yield series

        series = _filter()
        return TimeseriesCollectionView(series)

    def iter_range(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None
    ) -> Iterable[TimeseriesRow]:
        """Stream timestamp aligned data for the subscriptions in the view.
    
        The subscriptions are sorted according to their hash. Row indices align
        with the hash order.
        
        Args:
            start_time: The start range to iterate from.
            end_time: The end range to iterate to.
        
        Yields:
            row: A timeseries row.
        
        Raises:
            ValueError: If 'start_time' >= 'end_time'.
        """
        index, iters = [], []
        for series in self._series:
            index.append(hash(series.subscription))
            iters.append(series.iter_chunks(start_time, end_time))
        
        if not iters:
            return
        
        index, iters = zip(*sorted(zip(index, iters), key=lambda x: x[0]))

        iters = cast(List[Generator[Tuple[TimeChunk, Chunk], bool]], iters)

        yield index
        # Loop until we have exhausted all chunk iterators for each timeseries
        while True:
            timestamps: Set[datetime] = set()
            chunks: List[Tuple[TimeChunk, Chunk]] = []
            stop = 0
            for iter_ in iters:
                try:
                    chunk_tup = next(iter_)
                    chunks.append(chunk_tup)
                except StopIteration:
                    # Exhausted chunk iterator for this series
                    chunks.append(([], []))
                    stop += 1
            if stop == len(iters):
                # All chunk iterators exhausted, we're done
                break

            min_t = None
            for timechunk, _ in chunks:
                if timechunk:
                    min_t = timechunk[-1] if min_t is None or timechunk[-1] < min_t else min_t
                    timestamps.update(timechunk)

            for i in range(len(chunks)):
                timechunk = chunks[i][0]
                valchunk = chunks[i][1]
                # We can only iterate up to the minumum timestamp in the collection
                # of timeseries (min([chunk[0][-1] for chunk in chunks])).
                # If we go to max([chunk[0][-1] for chunk in chunks]) then the next
                # chunk for the series satisfying min([chunk[0][-1] for chunk in chunks])
                # may yield a duplicate or out of order timestamp.
                if timechunk and timechunk[-1] > min_t:
                    index = bisect.bisect_left(timechunk, min_t)
                    if timechunk[index] == min_t:
                        index += 1
                    if index == len(timechunk):
                        continue
                    elif index == 0:
                        iters[i].send((timechunk, valchunk))
                        chunks[i] = ([], [])
                    else:
                        iters[i].send((timechunk[index:], valchunk[index:]))
                        chunks[i] = (timechunk[:index], valchunk[:index])

            for timestamp in sorted(timestamps):
                if timestamp > min_t:
                    break
                row = []
                for timechunk, valchunk in chunks:
                    try:
                        if timechunk[0] == timestamp:
                            # The chunks are copies so we can pop without affecting
                            # the underlying dataset
                            row.append(valchunk.pop(0))
                            timechunk.pop(0)
                        else:
                            row.append(None)
                    except IndexError:
                        row.append(None)
                yield timestamp, row

    def __iter__(self) -> Iterable[TimeseriesRow]:
        for timestamp, row in self.iter_range():
            yield timestamp, row


timeseries_collection = TimeseriesCollection()