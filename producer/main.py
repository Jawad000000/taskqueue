import json
import time
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from shared.logger import audit_logger, get_recent_audit_logs, setup_logger
from shared.redis_client import (
    QUEUE_DEAD,
    QUEUE_DEFAULT,
    get_redis_client,
    get_system_metrics,
)
from shared.schemas import Job, QueueMetrics

logger = setup_logger("workqueue.producer")
app = FastAPI(
    title="WorkQueue Producer API",
    description="High-performance Distributed Background Task Processing Engine in Python",
    version="2.0.0",
)
redis_client = get_redis_client()
PRODUCER_START_TIME = time.time()


@app.get("/health")
async def health_check():
    """
    Health check endpoint verifying application uptime and Redis connectivity.
    """
    try:
        await redis_client.ping()
        return {
            "status": "healthy",
            "redis": "connected",
            "uptime_seconds": round(time.time() - PRODUCER_START_TIME, 2),
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=500, detail=f"Redis connection failed: {str(e)}"
        )


@app.post("/enqueue", status_code=202)
async def create_job(job_data: Job):
    """
    Ingests and validates a background task payload.
    Assigns a UUID, persists initial metadata in Redis, and pushes to FIFO queue.
    """
    # Specific task validation matching production expectations
    if job_data.type == "send_email":
        if "to" not in job_data.payload or "subject" not in job_data.payload:
            raise HTTPException(
                status_code=400,
                detail="Payload must contain 'to' and 'subject' fields for 'send_email' task",
            )
    elif job_data.type == "resize_image":
        if "width" not in job_data.payload and "new_x" not in job_data.payload:
            raise HTTPException(
                status_code=400,
                detail="Payload must contain target dimensions ('width' or 'new_x') for 'resize_image'",
            )

    job_dict = job_data.model_dump()
    job_serialized = json.dumps(job_dict)

    # Persist job state hash in Redis for fast O(1) status queries
    await redis_client.hset(
        f"job:{job_data.id}",
        mapping={
            "id": job_data.id,
            "type": job_data.type,
            "status": "pending",
            "retries": str(job_data.retries),
            "max_retries": str(job_data.max_retries),
            "created_at": str(job_data.created_at),
            "retry_count": "0",
            "payload": json.dumps(job_data.payload),
        },
    )

    # LPUSH onto the FIFO queue
    queue_len = await redis_client.lpush(QUEUE_DEFAULT, job_serialized)

    audit_logger.log_event(
        event="ENQUEUED",
        job_id=job_data.id,
        job_type=job_data.type,
        retries_left=job_data.retries,
        payload=job_data.payload,
        extra={"QueueDepth": queue_len},
    )
    logger.info(f"Enqueued job {job_data.id} [{job_data.type}] (Queue depth: {queue_len})")

    return {
        "status": "enqueued",
        "job_id": job_data.id,
        "type": job_data.type,
        "queue_depth": queue_len,
    }


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Retrieves full lifecycle state and execution metadata for a given job.
    """
    job_info = await redis_client.hgetall(f"job:{job_id}")
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")

    if "payload" in job_info:
        try:
            job_info["payload"] = json.loads(job_info["payload"])
        except Exception:
            pass

    return job_info


@app.get("/metrics", response_model=QueueMetrics)
async def get_metrics():
    """
    Real-time operational metrics for queue depths, completed jobs, failures, and active workers.
    """
    try:
        metrics_dict = await get_system_metrics(redis_client, PRODUCER_START_TIME)
        return QueueMetrics(**metrics_dict)
    except Exception as e:
        logger.error(f"Failed to fetch metrics: {e}")
        raise HTTPException(status_code=500, detail="Could not compute metrics")


@app.get("/dead-letter")
async def get_dead_letter_queue(limit: int = Query(default=100, ge=1, le=1000)):
    """
    Inspects failed tasks stored in the Dead Letter Queue (queue:dead).
    """
    raw_jobs = await redis_client.lrange(QUEUE_DEAD, 0, limit - 1)
    dead_jobs = []
    for raw in raw_jobs:
        try:
            dead_jobs.append(json.loads(raw))
        except Exception:
            dead_jobs.append({"raw": raw})
    return {"count": len(dead_jobs), "jobs": dead_jobs}


@app.get("/audit-logs")
async def get_audit_logs(limit: int = Query(default=50, ge=1, le=500)):
    """
    Returns recent structured audit events from logs/audit.log.
    """
    logs = get_recent_audit_logs(limit=limit)
    return {"count": len(logs), "logs": logs}
