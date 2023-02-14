from pydantic import BaseModel as PydanticBaseModel

from hyprxa.util.json import json_dumps, json_loads



class BaseModel(PydanticBaseModel):
    """BaseModel that uses `orjson` for serialization/deserialization."""
    class Config:
        json_dumps=json_dumps
        json_loads=json_loads