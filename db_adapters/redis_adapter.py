import aioredis
import logging
from qrm_db import QrmBaseDB, PREFIX_NAME


class RedisDB(QrmBaseDB):
    def __init__(self, redis_port: int = 6379):
        self.redis = aioredis.from_url(
            f"redis://localhost:{redis_port}", encoding="utf-8", decode_responses=True
        )

    async def get_all_keys_by_pattern(self, pattern: str = None) -> list:
        result = []
        async for key in self.redis.scan_iter(pattern):
            result.append(key)
        return result

    async def get_all_resources(self) -> list:
        return await self.get_all_keys_by_pattern(f'{PREFIX_NAME}*')

    async def add_resource(self, resource_name: str) -> None:
        await self.redis.rpush(f'{PREFIX_NAME}_{resource_name}', '')

    async def remove_resource(self, resource_name: str) -> bool:
        resource_name_db = f'{PREFIX_NAME}_{resource_name}'
        if await self.redis.delete(resource_name_db):
            return True
        logging.error(f'resource: {resource_name_db} doesn\'t exists in db')
        return False

    def __del__(self):
        self.redis.close()
