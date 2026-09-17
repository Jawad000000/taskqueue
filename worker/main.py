import asyncio
import json
import os
import signal
import time
from typing import List

from shared.logger import audit_logger, setup_logger
from shared.redis_client import (
    QUEUE_DEAD,
    QUEUE_DEFAULT,
    QUEUE_DELAYED,
    get_redis_client,
    incr_stat,
    register_worker_heartbeat,
)
from shared.schemas import Job
from worker.handlers import process_job, shutdown_cpu_pool

logger = setup_logger("workqueue.worker")
redis_client = get_redis_client()

# Configuration
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "3"))
HEARTBEAT_INTERVAL = 5.0
DELAYED_POLL_INTERVAL = 1.0

is_running = True
active_tasks: List[asyncio.Task] = []


def handle_shutdown(sig, frame):
    global is_running
    signame = signal.Signals(sig).name
    logger.info(f"Received {signame}. Initiating graceful shutdown (finishing in-flight tasks)...")
    is_running = False


async def delayed_queue_scheduler():
    """
    Polls the Redis Sorted Set (queue:delayed) for jobs whose backoff schedule has elapsed.
    Atomically migrates ready jobs into queue:default for immediate worker consumption.
    """
    logger.info("Delayed Queue Scheduler started. Monitoring exponential backoff jobs...")
    while is_running:
        try:
            now = time.time()
            # Fetch tasks whose scheduled execution time is <= now
            ready_jobs = await redis_client.zrangebyscore(
                QUEUE_DELAYED, min=0, max=now, start=0, num=25
            )

            if ready_jobs:
                for raw_job in ready_jobs:
                    # Remove atomically from delayed sorted set
                    removed = await redis_client.zrem(QUEUE_DELAYED, raw_job)
                    if removed:
                        await redis_client.lpush(QUEUE_DEFAULT, raw_job)
                        try:
                            data = json.loads(raw_job)
                            logger.info(
                                f"Backoff expired for job {data.get('id')}. Re-queued to {QUEUE_DEFAULT}"
                            )
                        except Exception:
                            pass
        except Exception as e:
            if is_running:
                logger.error(f"Error in delayed queue scheduler: {e}")

        await asyncio.sleep(DELAYED_POLL_INTERVAL)


async def single_worker_loop(worker_id: str):
    """
    Individual worker coroutine polling Redis with BRPOP.
    Multiple instances run concurrently to form the worker pool.
    """
    logger.info(f"[{worker_id}] Online and listening for jobs on '{QUEUE_DEFAULT}'...")
    last_heartbeat = 0.0

    while is_running:
        try:
            now = time.time()
            if now - last_heartbeat > HEARTBEAT_INTERVAL:
                await register_worker_heartbeat(redis_client, worker_id)
                last_heartbeat = now

            # Non-blocking pop with 2-second timeout to allow responsive shutdown checks
            result = await redis_client.brpop(QUEUE_DEFAULT, timeout=2)
            if result is None:
                continue

            _, serialized_job = result
            job_data = json.loads(serialized_job)
            job = Job(**job_data)

            start_time = time.time()
            job.started_at = start_time
            job.worker_id = worker_id
            job.status = "processing"

            # Mark status in Redis
            await redis_client.hset(
                f"job:{job.id}",
                mapping={
                    "status": "processing",
                    "started_at": str(start_time),
                    "worker_id": worker_id,
                },
            )
            logger.info(f"[{worker_id}] Processing job {job.id} [{job.type}]")

            try:
                # Execute task (CPU or I/O dispatched)
                result_msg = await process_job(job)
                duration_ms = (time.time() - start_time) * 1000.0

                # Mark completed
                job.status = "completed"
                job.completed_at = time.time()
                await redis_client.hset(
                    f"job:{job.id}",
                    mapping={
                        "status": "completed",
                        "completed_at": str(job.completed_at),
                    },
                )
                await incr_stat(redis_client, "completed")

                audit_logger.log_event(
                    event="SUCCESS",
                    job_id=job.id,
                    job_type=job.type,
                    worker_id=worker_id,
                    duration_ms=duration_ms,
                    retries_left=job.retries,
                    payload=job.payload,
                    extra={"Result": result_msg},
                )
                logger.info(f"[{worker_id}] Completed {job.id} in {duration_ms:.1f}ms: {result_msg}")

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000.0
                error_msg = str(e)
                logger.error(f"[{worker_id}] Error processing job {job.id}: {error_msg}")

                job.retries -= 1
                job.retry_count += 1
                job.last_error = error_msg

                if job.retries > 0:
                    # Exponential backoff calculation: 2^(retry_count) seconds (e.g. 2s, 4s, 8s)
                    delay_seconds = 2 ** job.retry_count
                    execute_at = time.time() + delay_seconds
                    job.status = "retrying"

                    await redis_client.hset(
                        f"job:{job.id}",
                        mapping={
                            "status": "retrying",
                            "retries": str(job.retries),
                            "retry_count": str(job.retry_count),
                            "last_error": error_msg,
                        },
                    )
                    # Place into delayed sorted set with target timestamp score
                    await redis_client.zadd(
                        QUEUE_DELAYED, {json.dumps(job.model_dump()): execute_at}
                    )
                    await incr_stat(redis_client, "retried")

                    audit_logger.log_event(
                        event="RETRY",
                        job_id=job.id,
                        job_type=job.type,
                        worker_id=worker_id,
                        duration_ms=duration_ms,
                        retries_left=job.retries,
                        error=error_msg,
                        extra={"NextRetryIn": f"{delay_seconds}s"},
                    )
                    logger.warning(
                        f"[{worker_id}] Job {job.id} failed. Retrying in {delay_seconds}s ({job.retries} attempts left)"
                    )
                else:
                    # Dead Letter Queue routing upon retry exhaustion
                    job.status = "dead"
                    await redis_client.hset(
                        f"job:{job.id}",
                        mapping={
                            "status": "dead",
                            "retries": "0",
                            "last_error": error_msg,
                        },
                    )
                    await redis_client.lpush(QUEUE_DEAD, json.dumps(job.model_dump()))
                    await incr_stat(redis_client, "failed")
                    await incr_stat(redis_client, "dlq_count")

                    audit_logger.log_event(
                        event="DLQ",
                        job_id=job.id,
                        job_type=job.type,
                        worker_id=worker_id,
                        duration_ms=duration_ms,
                        retries_left=0,
                        error=error_msg,
                    )
                    logger.critical(
                        f"[{worker_id}] Job {job.id} retries exhausted. Moved to Dead Letter Queue ({QUEUE_DEAD})"
                    )

        except Exception as e:
            if is_running:
                logger.error(f"[{worker_id}] Unexpected error in worker loop: {e}")
                await asyncio.sleep(1)

    logger.info(f"[{worker_id}] Worker loop exited cleanly.")


async def start_worker_pool():
    """
    Orchestrates the concurrent worker pool and scheduler tasks.
    """
    global active_tasks
    logger.info(f"Starting WorkQueue Worker Pool with CONCURRENCY={CONCURRENCY} workers...")

    # Spawn concurrent worker coroutines
    worker_tasks = [
        asyncio.create_task(single_worker_loop(f"worker-{i+1}"))
        for i in range(CONCURRENCY)
    ]
    scheduler_task = asyncio.create_task(delayed_queue_scheduler())

    active_tasks = worker_tasks + [scheduler_task]

    try:
        await asyncio.gather(*active_tasks)
    except asyncio.CancelledError:
        logger.info("Worker tasks cancelled during shutdown.")
    finally:
        shutdown_cpu_pool()
        logger.info("Worker pool shutdown complete.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        asyncio.run(start_worker_pool())
    except KeyboardInterrupt:
        pass