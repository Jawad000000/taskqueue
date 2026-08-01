import json

from fastapi import FastAPI
from shared.redis_client import get_redis_client
from shared.schemas import Job

app = FastAPI()
redis_client = get_redis_client()


@app.post("/enqueue")
async def create_job(job_data: Job):
    job_task = json.dumps(job_data.model_dump())
    await redis_client.lpush("queue:default", job_task)
    return {"status": "enqueued", "job_id": job_data.id}


@app.get("/dead-letter")
async def get_dead_letter_queue():
    raw_jobs = await redis_client.lrange("queue:dead", 0, -1)
    dead_jobs = [json.loads(j) for j in raw_jobs]
    return {"count": len(dead_jobs), "jobs": dead_jobs}


