import json

from fastapi import FastAPI, HTTPException
from shared.redis_client import get_redis_client
from shared.schemas import Job

app = FastAPI()
redis_client = get_redis_client()


@app.post("/enqueue")
async def create_job(job_data: Job):
    job_task = json.dumps(job_data.model_dump())

    # Store initial job metadata and status in Redis hash
    await redis_client.hset(
        f"job:{job_data.id}",
        mapping={
            "id": job_data.id,
            "type": job_data.type,
            "status": "pending",
            "retries": str(job_data.retries),
        },
    )

    await redis_client.lpush("queue:default", job_task)
    return {"status": "enqueued", "job_id": job_data.id}


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    job_info = await redis_client.hgetall(f"job:{job_id}")
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_info


@app.get("/dead-letter")
async def get_dead_letter_queue():
    raw_jobs = await redis_client.lrange("queue:dead", 0, -1)
    dead_jobs = [json.loads(j) for j in raw_jobs]
    return {"count": len(dead_jobs), "jobs": dead_jobs}



