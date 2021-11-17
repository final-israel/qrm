# sudo docker run -p 6379:6379 --name new-redis -d redis

import asyncio
import aioredis


async def redis_pool():
    # Redis client bound to pool of connections (auto-reconnecting).
    redis = aioredis.from_url(
        "redis://localhost", encoding="utf-8", decode_responses=True
    )
    await redis.set("my-key", "my-val")
    await redis.acl_list()
    val = await redis.get("my-key")
    print(val)
    await redis.hset("my_dict", "k1", "v1")
    val2 = await redis.hget("my_dict", "k1")
    print(val2)


if __name__ == "__main__":
    asyncio.run(redis_pool())
