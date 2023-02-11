from enum import Enum

from pydantic import BaseModel



class StatusOptions(str, Enum):
    OK = "OK"
    FAILED = "FAILED"


class Status(BaseModel):
    """Response model for operation status responses."""
    status: StatusOptions