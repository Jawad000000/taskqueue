from uuid import uuid4

from pydantic import BaseModel, Field


class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: str
    retries: int = 3
    payload: dict