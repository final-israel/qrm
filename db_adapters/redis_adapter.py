import time

import aioredis
import asyncio
import async_timeout
import json
import logging
from qrm_defs import resource_definition
from qrm_defs.resource_definition import Resource, ALLOWED_SERVER_STATUSES, ResourcesRequest, ResourcesRequestResponse
from db_adapters.qrm_db import QrmBaseDB
from typing import Dict, List


CHANNEL_RES_CHANGE_EVENT = 'channel:res_change_event'
PARTIAL_FILL_REQUESTS = 'fill_requests'
OPEN_REQUESTS = 'open_requests'
ALL_RESOURCES = 'all_resources'
SERVER_STATUS_IN_DB = 'qrm_status'
ACTIVE_STATUS = 'active'
TOKEN_RESOURCES_MAP = 'token_dict'
ACTIVE_TOKEN_DICT = 'active_token_dict'
LAST_REQ_RESP = 'last_req_resp'
PUBSUB_POLLING_TIME = 0.1


class RedisDB(QrmBaseDB):
    def __init__(self,
                 redis_port: int = 6379,
                 pubsub_polling_time: float = PUBSUB_POLLING_TIME):
        self.redis = aioredis.from_url(
            f"redis://localhost:{redis_port}", encoding="utf-8", decode_responses=True
        )
        self.res_status_change_event = {}  # type: Dict[str, asyncio.Event]
        self.pub_sub = self.redis.pubsub()
        self.pubsub_polling_time = pubsub_polling_time
        self.all_tasks = set()  # type: [asyncio.Task]
        pub_sub_task = asyncio.ensure_future(self.pubsub_reader())
        pub_sub_task.set_name('pub_sub_task')
        self.all_tasks.add(pub_sub_task)
        self.is_running = True

    async def pubsub_reader(self):
        await self.pub_sub.subscribe(CHANNEL_RES_CHANGE_EVENT)
        while self.is_running:
            try:
                async with async_timeout.timeout(PUBSUB_POLLING_TIME):
                    message = await self.pub_sub.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        try:
                            res_name = message.get('data')
                            self.res_status_change_event[res_name].set()
                            logging.info(f'got info from other redis adapter for resource '
                                         f'status change on resource {res_name}')
                        except KeyError as e:
                            self.res_status_change_event[res_name] = asyncio.Event()
                            self.res_status_change_event[res_name].set()
                    await asyncio.sleep(PUBSUB_POLLING_TIME)
            except asyncio.TimeoutError:
                pass
        await self.pub_sub.unsubscribe(CHANNEL_RES_CHANGE_EVENT)
        logging.info('done with pubsub reader')

    def init_params_blocking(self) -> None:
        asyncio.ensure_future(self.set_qrm_status(status=ACTIVE_STATUS))
        return

    async def init_default_params(self) -> None:
        await self.set_qrm_status(status=ACTIVE_STATUS)
        await self.init_events_for_resources()
        self.is_running = True

    async def init_events_for_resources(self) -> None:
        all_resources = await self.get_all_resources()
        for resource in all_resources:
            await self.init_event_for_resource(resource)

    async def init_event_for_resource(self, resource: Resource) -> None:
        self.res_status_change_event[resource.name] = asyncio.Event()
        if resource.status == ACTIVE_STATUS:
            self.res_status_change_event[resource.name].set()
        else:
            self.res_status_change_event[resource.name].clear()

    async def get_all_keys_by_pattern(self, pattern: str = None) -> List[Resource]:
        result = []
        async for key in self.redis.scan_iter(pattern):
            result.append(key)
        return result

    async def get_all_resources(self) -> List[Resource]:
        # return await self.get_all_keys_by_pattern(f'{RESOURCE_NAME_PREFIX}*')
        resources_list = []
        try:
            all_db_resources = await self.redis.hgetall(ALL_RESOURCES)
            for resource in all_db_resources.values():
                resources_list.append(resource_definition.resource_from_json(resource))
        except ValueError as e:
            if 'too many values to unpack' in e.args[0]:
                pass
            else:
                raise e
        return resources_list

    async def get_all_resources_dict(self) -> Dict[str, Resource]:
        """
        return: {resource_name1: Resource, resource_name2: resource)}
        """
        ret_dict = {}
        all_resources = await self.redis.hgetall(ALL_RESOURCES)
        for res_name, res_json in all_resources.items():
            ret_dict[res_name] = resource_definition.resource_from_json(res_json)
        return ret_dict

    async def add_resource(self, resource: Resource) -> bool:
        all_resources = await self.get_all_resources()
        if all_resources:
            if resource in all_resources:
                logging.warning(f'resource {resource.name} already exists')
                return False
        await self.redis.hset(ALL_RESOURCES, resource.name, resource.as_json())
        await self.redis.rpush(resource.db_name(), json.dumps({}))
        await self.init_event_for_resource(resource)
        return True

    async def get_resource_by_name(self, resource_name: str) -> Resource or None:
        resource_as_json = await self.redis.hget(ALL_RESOURCES, resource_name)
        if resource_as_json:
            return resource_definition.resource_from_json(resource_as_json)
        return None

    async def get_resources_by_names(self, resources_names: List[str]) -> List[Resource]:
        ret_list = []
        for res_name in resources_names:
            resource = await self.get_resource_by_name(res_name)
            if resource:
                ret_list.append(resource)
            else:
                logging.error(f'resource: {res_name} is not in DB')
        return ret_list

    async def remove_resource(self, resource: Resource) -> bool:
        if await self.redis.delete(resource.db_name()) and await self.redis.hdel(ALL_RESOURCES, resource.name):
            return True
        logging.error(f'resource: {resource.name} doesn\'t exists in db')
        return False

    async def set_resource_status(self, resource: Resource, status: str) -> bool:
        all_resources_dict = await self.get_all_resources_dict()
        if all_resources_dict.get(resource.name):
            resource_json = await self.redis.hget(ALL_RESOURCES, resource.name)
            resource_obj = resource_definition.resource_from_json(resource_json)
            resource_obj.status = status
            ret = await self.redis.hset(ALL_RESOURCES, resource.name, resource_obj.as_json())
            await self.set_event_for_resource(resource, status)
            return not ret
        else:
            return False

    async def set_event_for_resource(self, resource: Resource, status: str) -> None:
        try:
            if status == ACTIVE_STATUS:
                # self.res_status_change_event[resource.name].set()
                loop = asyncio.get_event_loop()
                loop.call_soon_threadsafe(self.res_status_change_event[resource.name].set)
                logging.info(f'set change event for resource {resource.name}')
                await self.redis.publish(CHANNEL_RES_CHANGE_EVENT, f'{resource.name}')
            else:
                self.res_status_change_event[resource.name].clear()
                logging.info(f'remove event for resource {resource.name}')
        except KeyError as e:
            self.res_status_change_event[resource.name] = asyncio.Event()
            await self.set_event_for_resource(resource, status)

    async def wait_for_resource_active_status(self, resource: Resource) -> None:
        try:
            await self.res_status_change_event[resource.name].wait()
            logging.info(f'done waiting for resource {resource.name} {ACTIVE_STATUS} status')
        except KeyError as e:
            self.res_status_change_event[resource.name] = asyncio.Event()
            self.res_status_change_event[resource.name].clear()
            await self.res_status_change_event[resource.name].wait()

    async def get_resource_status(self, resource: Resource) -> str:
        resource_json = await self.redis.hget(ALL_RESOURCES, resource.name)
        resource_obj = resource_definition.resource_from_json(resource_json)
        return resource_obj.status

    async def add_job_to_resource(self, resource: Resource, job: dict) -> bool:
        return await self.redis.lpush(resource.db_name(), json.dumps(job))

    async def get_resource_jobs(self, resource: Resource) -> List[Dict]:
        all_jobs = await self.redis.lrange(resource.db_name(), 0, -1)
        return self.build_resource_jobs_as_dicts(all_jobs)

    async def set_qrm_status(self, status: str) -> bool:
        if not self.validate_allowed_server_status(status):
            return False
        return await self.redis.set(SERVER_STATUS_IN_DB, status)

    async def get_qrm_status(self) -> str:
        return await self.redis.get(SERVER_STATUS_IN_DB)

    async def is_resource_exists(self, resource: Resource) -> bool:
        return resource in await self.get_all_resources()

    async def remove_job(self, token: str, resources_list: List[Resource] = None) -> List[Resource]:
        """
        this method remove job by it's id from list of resources or from all the resources in the DB
        :param token: the unique job id
        :param resources_list: list of all resources names to remove the job from.
        if this param is None, the job will be removed from all resources in the DB
        :return: True if
        """
        affected_resources = []

        if not resources_list:  # in this case remove the job from all the resources
            resources_list = await self.get_all_resources()
        for resource in resources_list:
            job = await self.get_job_for_resource_by_id(resource, token)
            if not job:
                continue
            else:
                await self.redis.lrem(resource.db_name(), 1, job)
                affected_resources.append(resource)

        return affected_resources

    async def get_job_for_resource_by_id(self, resource: Resource, token: str) -> str:
        resource_jobs = await self.get_resource_jobs(resource)
        for job in resource_jobs:
            if job.get('token') == token:
                return json.dumps(job)
        return ''

    async def get_active_job(self, resource: Resource) -> dict:
        active_job = await self.redis.lindex(resource.db_name(), -2)
        if active_job:
            return json.loads(active_job)
        else:
            return {}

    async def get_active_token_from_user_token(self, user_token: str) -> str:
        return await self.redis.hget(ACTIVE_TOKEN_DICT, user_token)

    async def set_active_token_for_user_token(self, user_token: str, active_token: str) -> bool:
        return await self.redis.hset(ACTIVE_TOKEN_DICT, user_token, active_token)

    async def set_token_for_resource(self, token: str, resource: Resource) -> None:
        resource_from_db = await self.get_resource_by_name(resource.name)
        if resource_from_db:
            resource_from_db.token = token
            await self.redis.hset(ALL_RESOURCES, resource.name, resource_from_db.as_json())
        else:
            logging.error(f'resource {resource.name} is not in DB, so can\'t add token to it')

    async def generate_token(self, token: str, resources: List[Resource]) -> bool:
        """
        This function will add token and its resources to Redis
        """
        if await self.redis.hget(TOKEN_RESOURCES_MAP, token):
            logging.error(f'token {token} already exists in DB, can\'t generate it again')
            return False
        resources_list = []
        for resource in resources:
            resources_list.append(resource.as_json())
            await self.set_token_for_resource(token, resource)
        logging.info(f'generate token {token} with {resources_list}')
        return await self.redis.hset(TOKEN_RESOURCES_MAP, token, json.dumps(resources_list))

    async def get_token_resources(self, token: str) -> List[Resource]:
        resources_list = []
        token_json = await self.redis.hget(TOKEN_RESOURCES_MAP, token)
        if not token_json:
            logging.warning(f'token {token} does not exists in db')
            return []
        for resource_json in json.loads(token_json):
            resources_list.append(resource_definition.resource_from_json(resource_json))
        return resources_list

    async def add_resources_request(self, resources_req: ResourcesRequest) -> None:
        await self.redis.hset(OPEN_REQUESTS, resources_req.token, resources_req.as_json())

    async def get_open_requests(self) -> Dict[str, ResourcesRequest]:
        open_requests = await self.redis.hgetall(OPEN_REQUESTS)
        ret_dict = {}
        for token, req in open_requests.items():
            ret_dict[token] = resource_definition.resource_request_from_json(req)
        return ret_dict

    async def get_open_request_by_token(self, token: str) -> ResourcesRequest:
        # use this method if you know the token request since it's much faster than get_open_requests
        open_req = await self.redis.hget(OPEN_REQUESTS, token)
        if open_req:
            return resource_definition.resource_request_from_json(open_req)
        else:
            return ResourcesRequest()

    async def update_open_request(self, token: str, updated_request: ResourcesRequest) -> bool:
        if await self.redis.hget(OPEN_REQUESTS, token):
            await self.redis.hset(OPEN_REQUESTS, token, updated_request.as_json())
            return True
        else:
            logging.error(f'request with token {token} is not in DB!')
            return False

    async def remove_open_request(self, token: str) -> None:
        if await self.redis.hget(OPEN_REQUESTS, token):
            await self.redis.hdel(OPEN_REQUESTS, token)
        else:
            logging.warning(f'request with token {token} is not in DB!')

    async def partial_fill_request(self, token: str, resource: Resource) -> None:
        partial_fill_req = await self.redis.hget(PARTIAL_FILL_REQUESTS, token)
        if partial_fill_req:
            partial_fill_list = json.loads(partial_fill_req)
            if resource.name in partial_fill_list:
                return
            partial_fill_list.append(resource.name)
            await self.redis.hset(PARTIAL_FILL_REQUESTS, token, json.dumps(partial_fill_list))
            rrr = ResourcesRequestResponse(
                token=token,
                names=partial_fill_list
            )
            await self.set_req_resp(rrr)
        else:
            await self.redis.hset(PARTIAL_FILL_REQUESTS, token, json.dumps([resource.name]))
            rrr = ResourcesRequestResponse(
                token=token,
                names=[resource.name]
            )
            await self.set_req_resp(rrr)

    async def get_partial_fill(self, token: str) -> ResourcesRequestResponse:
        partial_fill_req = await self.redis.hget(PARTIAL_FILL_REQUESTS, token)
        if partial_fill_req:
            return ResourcesRequestResponse(json.loads(partial_fill_req), token)
        else:
            return ResourcesRequestResponse()

    async def remove_partially_fill_request(self, token: str) -> None:
        await self.redis.hdel(PARTIAL_FILL_REQUESTS, token)

    async def is_request_filled(self, token: str) -> bool:
        token_in_map = await self.redis.hget(TOKEN_RESOURCES_MAP, token)
        token_in_open_req = await self.redis.hget(OPEN_REQUESTS, token)
        all_tokens = await self.redis.hgetall(TOKEN_RESOURCES_MAP)
        logging.debug(f'all tokens are: {all_tokens}')
        logging.debug(f'token_in_map: {token_in_map}, token_in_open_req: {token_in_open_req}')
        if token_in_map and not token_in_open_req:
            return True
        return False

    async def get_req_resp_for_token(self, token: str) -> ResourcesRequestResponse:
        rrr = await self.redis.hget(LAST_REQ_RESP, token)
        if not rrr:  # no response for token, return response with relevant msg
            return ResourcesRequestResponse(token=token, message='no response for token')
        resp = ResourcesRequestResponse.from_json(rrr)
        return resp

    async def set_req_resp(self, rrr: ResourcesRequestResponse) -> None:
        await self.redis.hset(LAST_REQ_RESP, rrr.token, rrr.as_json())

    async def get_all_open_tokens(self) -> List[str]:
        # these are the tokens used for recovery.
        # it contains both active requests waiting in queues and
        # the totally filled requests
        tokens_list = list()

        token_res_map = await self.redis.hgetall(TOKEN_RESOURCES_MAP)
        tokens_list.extend(token_res_map.keys())

        open_req = await self.redis.hgetall(OPEN_REQUESTS)
        tokens_list.extend(open_req.keys())

        part_fill = await self.redis.hgetall(PARTIAL_FILL_REQUESTS)
        tokens_list.extend(part_fill.keys())

        return list(set(tokens_list))

    async def close(self) -> None:
        self.is_running = False
        await asyncio.sleep(2 * self.pubsub_polling_time)  # to allow gracefully shutdown

        # for task in self.all_tasks:
        #     task.cancel()
        #     try:
        #         await task
        #     except asyncio.CancelledError as e:
        #         pass

        await self.pub_sub.close()
        await self.redis.close()
        return

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

    # def __del__(self):
    #     for task in self.all_tasks:
    #         if not task.cancelled():
    #             logging.info(f'cancelling task {task.get_name()}')
    #             task.cancel()
