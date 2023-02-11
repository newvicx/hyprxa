from datetime import datetime



class ChunkLimitError(IndexError):
    """Raised if an attempt is made to `append` to a full time series chunk."""
    def __init__(self, chunk_size: int) -> None:
        self.chunk_size = chunk_size
    def __str__(self) -> str:
        return "Cannot append to chunk. Chunk is full ({})".format(self.chunk_size)


class OldTimestampError(ValueError):
    """Raised if an attempt is made to add an old timestamp to a timeseries."""
    def __init__(self, timestamp: datetime) -> None:
        self.timestamp = timestamp
    def __str__(self) -> str:
        return "Timestamp cannot be older than the latest timestamp in the time series ({})".format(self.timestamp)