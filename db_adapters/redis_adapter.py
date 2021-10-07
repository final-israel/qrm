import aioredis
import asyncio
import json
import logging
import qrm_db

from qrm_db import QrmBaseDB, RESOURCE_NAME_PREFIX, ALLOWED_SERVER_STATUSES
from typing import Dict, List


SERVER_STATUS_IN_DB = 'qrm_status'


class RedisDB(QrmBaseDB):
    def __init__(self, redis_port: int = 6379):
        self.redis = aioredis.from_url(
            f"redis://localhost:{redis_port}", encoding="utf-8", decode_responses=True
        )
        self.init_default_params()

    def init_default_params(self) -> None:
        # TODO: validate if redis db already exists on server before init parameters
        task = asyncio.ensure_future(self.set_qrm_status(status='active'))

    async def get_all_keys_by_pattern(self, pattern: str = None) -> List[str]:
        result = []
        async for key in self.redis.scan_iter(pattern):
            result.append(key)
        return result

    async def get_all_resources(self) -> List[str]:
        return await self.get_all_keys_by_pattern(f'{RESOURCE_NAME_PREFIX}*')

    async def add_resource(self, resource_name: str) -> None:
        await self.redis.rpush(qrm_db.get_resource_name_in_db(resource_name), json.dumps({}))

    async def remove_resource(self, resource_name: str) -> bool:
        resource_name_db = qrm_db.get_resource_name_in_db(resource_name)
        if await self.redis.delete(resource_name_db):
            return True
        logging.error(f'resource: {resource_name_db} doesn\'t exists in db')
        return False

    async def set_resource_status(self, resource_name: str, status: str) -> bool:
        return await self.redis.set(qrm_db.get_resource_status_in_db(resource_name), status)

    async def get_resource_status(self, resource_name: str) -> str:
        return await self.redis.get(qrm_db.get_resource_status_in_db(resource_name))

    async def add_job_to_resource(self, resource_name: str, job: dict) -> bool:
        return await self.redis.lpush(qrm_db.get_resource_name_in_db(resource_name), json.dumps(job))

    async def get_resource_jobs(self, resource_name: str) -> List[Dict]:
        all_jobs = await self.redis.lrange(qrm_db.get_resource_name_in_db(resource_name), 0, -1)
        return self.build_resource_jobs_as_dicts(all_jobs)

    async def set_qrm_status(self, status: str) -> bool:
        if not self.validate_allowed_server_status(status):
            return False
        return await self.redis.set(SERVER_STATUS_IN_DB, status)

    async def get_qrm_status(self) -> None:
        return await self.redis.get(SERVER_STATUS_IN_DB)

    @staticmethod
    def validate_allowed_server_status(status: str) -> bool:
        if status not in ALLOWED_SERVER_STATUSES:
            logging.error(f'can\'t update qrm_server to status: {status}, '
                          f'allowed statuses are: {ALLOWED_SERVER_STATUSES}')
            return False
        return True

    @staticmethod
    def build_resource_jobs_as_dicts(jobs_list: List[str]) -> List[Dict]:
        ret_list = []
        for job in jobs_list:
            ret_list.append(json.loads(job))
        return ret_list

    def __del__(self):
        self.redis.close()
