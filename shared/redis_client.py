import os
import time
from typing import Any, Dict, Optional
from redis.asyncio import Redis

# Keys configuration
QUEUE_DEFAULT = "queue:default"
QUEUE_DEAD = "queue:dead"
QUEUE_DELAYED = "queue:delayed"
STATS_KEY = "workqueue:stats"
WORKERS_SET = "workqueue:workers"

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)


def get_redis_client() -> Redis:
    return redis_client


async def incr_stat(client: Redis, field: str, amount: int = 1) -> int:
    """
    Atomically increments a statistic counter in Redis to prevent data races.
    """
    return await client.hincrby(STATS_KEY, field, amount)


async def get_all_stats(client: Redis) -> Dict[str, int]:
    """
    Retrieves all cumulative statistics from Redis.
    """
    raw_stats = await client.hgetall(STATS_KEY)
    return {
        "completed": int(raw_stats.get("completed", 0)),
        "failed": int(raw_stats.get("failed", 0)),
        "retried": int(raw_stats.get("retried", 0)),
        "dlq_count": int(raw_stats.get("dlq_count", 0)),
    }


async def register_worker_heartbeat(client: Redis, worker_id: str, ttl: int = 15):
    """
    Registers or refreshes a worker's heartbeat in Redis with a TTL.
    """
    pipe = client.pipeline()
    pipe.sadd(WORKERS_SET, worker_id)
    pipe.set(f"worker:heartbeat:{worker_id}", str(time.time()), ex=ttl)
    await pipe.execute()


async def get_active_workers_count(client: Redis) -> int:
    """
    Calculates number of active workers with fresh heartbeats.
    Cleans up stale workers.
    """
    workers = await client.smembers(WORKERS_SET)
    active = 0
    stale = []
    for w_id in workers:
        exists = await client.exists(f"worker:heartbeat:{w_id}")
        if exists:
            active += 1
        else:
            stale.append(w_id)
    if stale:
        await client.srem(WORKERS_SET, *stale)
    return active


async def get_system_metrics(client: Redis, start_time: float) -> Dict[str, Any]:
    """
    Returns a unified snapshot of system metrics.
    """
    pipe = client.pipeline()
    pipe.llen(QUEUE_DEFAULT)
    pipe.zcard(QUEUE_DELAYED)
    pipe.llen(QUEUE_DEAD)
    results = await pipe.execute()

    q_default_len, q_delayed_len, q_dead_len = results
    stats = await get_all_stats(client)
    active_workers = await get_active_workers_count(client)

    return {
        "total_jobs_in_queue": q_default_len,
        "jobs_delayed": q_delayed_len,
        "jobs_in_dlq": q_dead_len,
        "jobs_completed": stats["completed"],
        "jobs_failed": stats["failed"],
        "jobs_retried": stats["retried"],
        "active_workers": active_workers,
        "uptime_seconds": round(time.time() - start_time, 2),
    }