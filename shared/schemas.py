import time
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class Job(BaseModel):
    """
    Core Job Schema representing a distributed background task.
    Validated at ingestion time by Pydantic v2.
    """
    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique UUID identifier for the job")
    type: str = Field(..., description="Type of task to execute (e.g. send_email, resize_image, generate_pdf)")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary task parameters and arguments")
    retries: int = Field(default=3, ge=0, description="Remaining retry attempts before routing to Dead Letter Queue")
    max_retries: int = Field(default=3, ge=0, description="Original maximum retries configured for the job")
    status: str = Field(default="pending", description="Current lifecycle state (pending, processing, completed, retrying, dead)")
    created_at: float = Field(default_factory=time.time, description="Unix timestamp when job was ingested")
    started_at: Optional[float] = Field(default=None, description="Unix timestamp when worker started processing")
    completed_at: Optional[float] = Field(default=None, description="Unix timestamp when processing finished")
    worker_id: Optional[str] = Field(default=None, description="Identifier of the worker that processed the job")
    last_error: Optional[str] = Field(default=None, description="Error message if the last execution failed")
    retry_count: int = Field(default=0, ge=0, description="Number of times this job has been retried so far")


class QueueMetrics(BaseModel):
    """
    Snapshot of system performance and queue health.
    """
    total_jobs_in_queue: int = Field(..., description="Number of ready jobs in queue:default")
    jobs_delayed: int = Field(default=0, description="Number of jobs waiting in queue:delayed for backoff")
    jobs_in_dlq: int = Field(default=0, description="Number of dead jobs in queue:dead")
    jobs_completed: int = Field(default=0, description="Cumulative count of successfully executed jobs")
    jobs_failed: int = Field(default=0, description="Cumulative count of jobs that encountered errors")
    jobs_retried: int = Field(default=0, description="Cumulative count of retry attempts triggered")
    active_workers: int = Field(default=0, description="Reported active worker coroutines")
    uptime_seconds: float = Field(default=0.0, description="Uptime of the monitoring service")