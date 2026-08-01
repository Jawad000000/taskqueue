import asyncio
import json
import logging

from shared.redis_client import get_redis_client
from shared.schemas import Job
from worker.handlers import process_job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

redis_client = get_redis_client()


async def worker_loop():
    logger.info("Worker started. Listening for jobs on 'queue:default'...")

    while True:
        result = await redis_client.brpop("queue:default", timeout=5)

        if result is None:
            continue

        queue_name, serialized_job = result
        job_data = json.loads(serialized_job)
        job = Job(**job_data)

        # Mark status as processing in Redis
        await redis_client.hset(f"job:{job.id}", "status", "processing")

        try:
            await process_job(job)
            await redis_client.hset(f"job:{job.id}", "status", "completed")
            logger.info(f"Job completed successfully: {job.id}")
        except Exception as e:
            logger.error(f"Error processing job {job.id}: {e}")
            job = job.model_copy(
                update={"retries": job.retries - 1, "status": "failed"}
            )
            if job.retries > 0:
                job_dec = json.dumps(job.model_dump())
                await redis_client.hset(
                    f"job:{job.id}",
                    mapping={"status": "retrying", "retries": str(job.retries)},
                )
                await redis_client.lpush("queue:default", job_dec)
                logger.info(f"Retrying job {job.id}, retries_left: {job.retries}")
            else:
                job_dec = json.dumps(job.model_dump())
                await redis_client.hset(f"job:{job.id}", "status", "dead")
                await redis_client.lpush("queue:dead", job_dec)
                logger.warning(
                    f"Job {job.id} retries exhausted. Moved to Dead Letter Queue (queue:dead)."
                )


if __name__ == "__main__":
    asyncio.run(worker_loop())