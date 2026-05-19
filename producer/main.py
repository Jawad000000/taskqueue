from fastapi import FastAPI
from shared.schemas import Job
from shared.redis_client import get_redis_client
import json

app = FastAPI()
redis_client = get_redis_client()

@app.post("/enqueue")
async def create_job(job_data: Job):
    job_task = json.dumps(job_data.model_dump())
    await redis_client.lpush("queue:default", job_task)
    return {"status": "enqueued", "job_id": job_data.id}

