from datetime import datetime

from pydantic import BaseModel as PydanticBaseModel

from hyprxa.util.json import json_dumps, json_loads



class BaseModel(PydanticBaseModel):
    """BaseModel that uses `orjson` for serialization/deserialization."""
    class Config:
        json_dumps=json_dumps
        json_loads=json_loads


class WorkerInfo(BaseModel):
    """Model for MongoDB worker statistics."""
    running: bool
    stopped: bool
    queue_length: int
    pending_batch_length: int
    pending_batch_size: int


class StorageHandlerInfo(BaseModel):
    """Model for storage handler statistics."""
    created: datetime
    uptime: int
    workers_used: int
    worker: WorkerInfo | None