import os
from redis.asyncio import Redis

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=6379,
    decode_responses=True,
)


def get_redis_client() -> Redis:
    return redis_client