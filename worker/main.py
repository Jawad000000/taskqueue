import asyncio
import json

from shared.schemas import Job
from shared.redis_client import get_redis_client
from worker.handlers import process_job

redis_client = get_redis_client()
async def worker_loop():
    

    while True:
        result = await redis_client.brpop("queue:default", timeout=5)

        if result is None:
            continue

        queue_name, serialized_job = result

        job_data = json.loads(serialized_job)

        job = Job(**job_data)

        try:
            await process_job(job)
            print(f"Job done: {job.id}")
        except Exception as e:
            job = job.model_copy(update={"retries": job.retries - 1})
            if job.retries >0:
                job_dec= json.dumps(job.model_dump())
                await redis_client.lpush("queue:default", job_dec)
                print(f"Retrying job {job.id}, retries_left: {job.retries}")
            else:
                job_dec = json.dumps(job.model_dump())
                await redis_client.lpush("queue:dead", job_dec)
                print(f"Job {job.id} is dead. Moved to Dead Letter Queue (queue:dead).")



        


if __name__ == "__main__":
    asyncio.run(worker_loop())