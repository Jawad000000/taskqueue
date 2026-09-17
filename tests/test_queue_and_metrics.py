import asyncio
import json
import time
import pytest
from fakeredis.aioredis import FakeRedis
from shared.redis_client import (
    QUEUE_DEAD,
    QUEUE_DEFAULT,
    QUEUE_DELAYED,
    get_active_workers_count,
    get_all_stats,
    get_system_metrics,
    incr_stat,
    register_worker_heartbeat,
)
from shared.schemas import Job


@pytest.fixture
def fake_redis():
    return FakeRedis(decode_responses=True)


@pytest.mark.asyncio
async def test_fifo_queue_ordering(fake_redis):
    # Producer: LPUSH jobs
    job1 = Job(type="send_email", payload={"to": "first@test.com", "subject": "1"})
    job2 = Job(type="send_email", payload={"to": "second@test.com", "subject": "2"})

    await fake_redis.lpush(QUEUE_DEFAULT, json.dumps(job1.model_dump()))
    await fake_redis.lpush(QUEUE_DEFAULT, json.dumps(job2.model_dump()))

    assert await fake_redis.llen(QUEUE_DEFAULT) == 2

    # Worker: BRPOP jobs (pops from right = FIFO)
    res1 = await fake_redis.brpop(QUEUE_DEFAULT, timeout=1)
    res2 = await fake_redis.brpop(QUEUE_DEFAULT, timeout=1)

    assert json.loads(res1[1])["payload"]["to"] == "first@test.com"
    assert json.loads(res2[1])["payload"]["to"] == "second@test.com"
    assert await fake_redis.llen(QUEUE_DEFAULT) == 0


@pytest.mark.asyncio
async def test_atomic_metrics(fake_redis):
    await incr_stat(fake_redis, "completed", 5)
    await incr_stat(fake_redis, "failed", 2)
    await incr_stat(fake_redis, "retried", 4)
    await incr_stat(fake_redis, "dlq_count", 1)

    stats = await get_all_stats(fake_redis)
    assert stats["completed"] == 5
    assert stats["failed"] == 2
    assert stats["retried"] == 4
    assert stats["dlq_count"] == 1


@pytest.mark.asyncio
async def test_worker_heartbeats(fake_redis):
    await register_worker_heartbeat(fake_redis, "worker-1", ttl=10)
    await register_worker_heartbeat(fake_redis, "worker-2", ttl=10)

    count = await get_active_workers_count(fake_redis)
    assert count == 2


@pytest.mark.asyncio
async def test_delayed_queue_sorting_and_migration(fake_redis):
    now = time.time()
    job_ready = Job(type="send_email", payload={"to": "ready@test.com", "subject": "ready"})
    job_future = Job(type="send_email", payload={"to": "future@test.com", "subject": "future"})

    # Schedule ready job for now-1s, future job for now+60s
    await fake_redis.zadd(QUEUE_DELAYED, {json.dumps(job_ready.model_dump()): now - 1})
    await fake_redis.zadd(QUEUE_DELAYED, {json.dumps(job_future.model_dump()): now + 60})

    assert await fake_redis.zcard(QUEUE_DELAYED) == 2

    # Query items ready (score <= now)
    ready = await fake_redis.zrangebyscore(QUEUE_DELAYED, min=0, max=now)
    assert len(ready) == 1

    # Migrate to default queue
    await fake_redis.zrem(QUEUE_DELAYED, ready[0])
    await fake_redis.lpush(QUEUE_DEFAULT, ready[0])

    assert await fake_redis.zcard(QUEUE_DELAYED) == 1
    assert await fake_redis.llen(QUEUE_DEFAULT) == 1


@pytest.mark.asyncio
async def test_system_metrics_snapshot(fake_redis):
    start = time.time() - 10
    await fake_redis.lpush(QUEUE_DEFAULT, "dummy_task")
    await fake_redis.lpush(QUEUE_DEAD, "dead_task")
    await incr_stat(fake_redis, "completed", 12)

    metrics = await get_system_metrics(fake_redis, start)
    assert metrics["total_jobs_in_queue"] == 1
    assert metrics["jobs_in_dlq"] == 1
    assert metrics["jobs_completed"] == 12
    assert metrics["uptime_seconds"] >= 10.0
