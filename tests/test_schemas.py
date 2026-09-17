import pytest
from pydantic import ValidationError
from shared.schemas import Job, QueueMetrics


def test_job_defaults():
    job = Job(type="send_email", payload={"to": "user@test.com", "subject": "Hello"})
    assert job.id is not None
    assert len(job.id) == 36  # UUID4 format
    assert job.type == "send_email"
    assert job.retries == 3
    assert job.max_retries == 3
    assert job.status == "pending"
    assert job.retry_count == 0
    assert job.created_at > 0
    assert job.started_at is None
    assert job.completed_at is None


def test_job_validation_missing_type():
    with pytest.raises(ValidationError):
        Job(payload={})


def test_queue_metrics_schema():
    metrics = QueueMetrics(
        total_jobs_in_queue=10,
        jobs_delayed=2,
        jobs_in_dlq=1,
        jobs_completed=50,
        jobs_failed=3,
        jobs_retried=5,
        active_workers=3,
        uptime_seconds=120.5,
    )
    assert metrics.total_jobs_in_queue == 10
    assert metrics.jobs_delayed == 2
    assert metrics.jobs_in_dlq == 1
    assert metrics.jobs_completed == 50
    assert metrics.active_workers == 3
