import asyncio
import logging
from redis_adapter import RedisDB
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName, \
    generate_token_from_seed
from typing import List, Dict
from abc import ABC, abstractmethod
CANCELED = "canceled"

REDIS_PORT = 6379
ResourcesListType = List[Resource]


class QRMEvent(asyncio.Event):
    def __init__(self):
        super().__init__()
        self.reason = None

    def set(self, reason=None):
        self.reason = reason
        asyncio.Event.set(self)


class QrmIfc(ABC):
    # this class is the interface between the QueueManagerBackEnd and the qrm_http_server
    # the qrm_http_server will only call methods from the interface
    @abstractmethod
    async def cancel_request(self, token: str) -> None:
        pass

    @abstractmethod
    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        pass

    @abstractmethod
    async def is_request_active(self, token: str) -> bool:
        pass

    @abstractmethod
    async def get_new_token(self, token: str) -> str:
        pass

    @abstractmethod
    async def get_filled_request(self, token: str) -> ResourcesRequestResponse:
        pass


class QueueManagerBackEnd(QrmIfc):
    def __init__(self, redis_port: int = REDIS_PORT):
        if redis_port:
            self.redis = RedisDB(redis_port)
        else:
            self.redis = RedisDB(REDIS_PORT)
        self.tokens_change_event = {}  # type: Dict[str, QRMEvent]

    # Recovery from DB
    async def init_tokens_events(self) -> None:
        open_requests = await self.redis.get_open_requests()
        for token, in open_requests.keys():
            self.tokens_change_event[token].set()

    async def names_worker(self, token: str) -> ResourcesRequestResponse:
        user_req = await self.redis.get_open_request_by_token(token)
        for resources_list_request in user_req.names:
            remaining_resources = resources_list_request.count
            while remaining_resources > 0:
                logging.info(f'remaining resources for token: {token} is: {remaining_resources}')
                remaining_resources = await self.find_available_resources_by_names(remaining_resources,
                                                                                   resources_list_request, token)
                logging.info(f'waiting for signal on token: {token}')
                if remaining_resources != 0:
                    reason = await self.worker_wait_for_continue_event(token)
                    if reason == CANCELED:
                        return ResourcesRequestResponse()

        logging.info(f'done handling token: {token}')
        return await self.finalize_filled_request(token)

    async def finalize_filled_request(self, token: str):
        await self.redis.remove_open_request(token)
        response = await self.redis.get_partial_fill(token)
        resources_list = await self.redis.get_resources_by_names(response.names)
        await self.redis.generate_token(token, resources_list)
        return response

    async def worker_wait_for_continue_event(self, token: str) -> str:
        self.tokens_change_event[token].clear()
        await self.tokens_change_event[token].wait()

        return self.tokens_change_event[token].reason

    async def find_available_resources_by_names(self, remaining_resources: int, resources_list_request: ResourcesByName,
                                                token: str):
        matched_resources = []
        for resource_name in resources_list_request.names:
            resource = await self.redis.get_resource_by_name(resource_name)
            active_job = await self.redis.get_active_job(resource)
            logging.info(f'active job for resource: {resource_name} is: {active_job.get("token")}')
            if active_job.get('token') == token and remaining_resources > 0:
                await self.redis.set_token_for_resource(token, resource)
                await self.redis.partial_fill_request(token, resource)
                matched_resources.append(resource_name)
                remaining_resources -= 1
        # TODO: replace this for loop with nicer one:
        for res_name in matched_resources:
            if res_name in resources_list_request.names:
                resources_list_request.names.remove(res_name)
        return remaining_resources

    async def remove_matched_resources(self, matched_resources, resources_list_request):
        for res in matched_resources:
            if res in resources_list_request.names:
                resources_list_request.names.remove(res)

    async def generate_jobs_from_names_request(self, token: str):
        user_req = await self.redis.get_open_request_by_token(token)
        for req_by_name in user_req.names:
            for res_name in req_by_name.names:
                resource = await self.redis.get_resource_by_name(res_name)
                await self.generate_job(resource, user_req.token)

    async def cancel_request(self, token: str) -> None:

        affected_resources = await self.redis.remove_job(token=token)
        for resource in affected_resources:
            ret = await self.redis.get_active_job(resource)
            if "token" not in ret:
                continue

            affected_token = ret["token"]
            # release coros
            self.tokens_change_event[affected_token].set()

        self.tokens_change_event[token].set(reason=CANCELED)

    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        requested_token = resources_request.token
        all_resources_dict = await self.redis.get_all_resources_dict()
        resources_token_list = await self.redis.get_token_resources(requested_token)
        if self.is_token_valid(requested_token, all_resources_dict, resources_token_list):
            await self.redis.set_active_token_for_user_token(
                requested_token, requested_token
            )
            return await self.handle_token_request_for_valid_token(requested_token, resources_token_list)

        active_token = generate_token_from_seed(requested_token)
        await self.redis.set_active_token_for_user_token(
            requested_token, active_token
        )

        await self.init_event_for_token(active_token)
        if resources_request.names:
            result = await self.handle_names_request(all_resources_dict, resources_request, requested_token,
                                                     active_token)
            return result

        return ResourcesRequestResponse(token=requested_token)  # return empty response

    async def handle_names_request(self, all_resources_dict: Dict[str, Resource], resources_request: ResourcesRequest,
                                   requested_token: str, active_token: str):
        await self.reorder_names_request(requested_token, resources_request.names, all_resources_dict)
        resources_request.token = active_token
        await self.redis.add_resources_request(resources_request)
        await self.generate_jobs_from_names_request(active_token)
        return await self.names_worker(active_token)

    async def init_event_for_token(self, token) -> None:
        self.tokens_change_event[token] = QRMEvent()
        self.tokens_change_event[token].set()

    async def reorder_names_request(self, old_token: str, resources_by_name: List[ResourcesByName],
                                    all_resources_dict: Dict[str, Resource]) -> None:
        """
        this method prioritize the resources order by checking their old token and put all the resources with the old
        active token at the beginning of the resources_by_names
        :param old_token: the old token of the request which is no longer valid
        :param resources_by_name: List[ResourcesByName] as requested by client
        :param all_resources_dict: all resources dictionary from the db
        :return: None, the method changes the resources_by_name structure
        """
        original_resources = await self.redis.get_token_resources(old_token)
        current_resources_names_with_old_token = []
        for res in original_resources:
            if res.token == old_token:  # all resources that currently hold this token
                current_resources_names_with_old_token.append(res.name)
        for resources_req in resources_by_name:
            tmp_list_active_token = []
            tmp_list_non_active_token = []
            for resource_name in resources_req.names:
                if not all_resources_dict.get(resource_name):
                    logging.error(f'got request for resource which is not in DB: {resource_name}')
                # first try to add it to the resources with the same token to preserve system steady state:
                elif resource_name in current_resources_names_with_old_token:
                    # find the resource index and move it to the beginning of the list, so it will be handled first:
                    tmp_list_active_token.append(resource_name)
                else:
                    tmp_list_non_active_token.append(resource_name)
            resources_req.names = tmp_list_active_token + tmp_list_non_active_token

    def find_one_resource(self, resource: Resource, all_resources_list: ResourcesListType) -> Resource or None:
        list_of_resources_with_token = self.find_all_resources_with_token(resource.token, all_resources_list)
        if len(list_of_resources_with_token) == 1:
            for one_resource in list_of_resources_with_token:
                return one_resource
        elif len(list_of_resources_with_token) == 0:
            return None
        else:
            raise NotImplemented

    async def handle_token_request_for_valid_token(self, token: str, resources_token_list: List[Resource]) \
            -> ResourcesRequestResponse:
        # this method assumes that the token is valid and all resources are available with this token
        resources_request_resp = ResourcesRequestResponse()
        resources_request_resp.token = token
        for resource in resources_token_list:
            await self.generate_job(resource, token)
            resources_request_resp.names.append(resource.name)
        return resources_request_resp

    async def generate_job(self, resource, token):
        await self.redis.add_job_to_resource(resource, {'token': token})

    async def is_request_active(self, token: str) -> bool:
        # request is active if it's not filled, or it's already cancelled:
        is_filled = await self.redis.is_request_filled(token)
        is_cancelled = self.tokens_change_event[token].reason == CANCELED
        logging.info(f'request for token: {token} cancelled: {is_cancelled}, '
                     f'filled: {is_filled}')
        return not (is_filled or is_cancelled)

    async def get_new_token(self, token: str) -> str:
        new_token = await self.redis.get_active_token_from_user_token(token)
        while not new_token:
            await asyncio.sleep(0.1)
            new_token = await self.redis.get_active_token_from_user_token(token)
        return new_token

    async def get_filled_request(self, token: str) -> ResourcesRequestResponse:
        # if the request is not totally filled, you will get the current partial fill.
        # in case you want only totally filled, first check is_request_active method
        return await self.redis.get_partial_fill(token)

    @staticmethod
    def is_token_valid(token: str, resources_dict: Dict[str, Resource],
                       original_resources_token_list: List[Resource]) -> bool:
        if not original_resources_token_list:
            return False
        for orig_resource_in_group in original_resources_token_list:
            resource_obj = resources_dict.get(orig_resource_in_group.name)
            if resource_obj:
                if resource_obj.token != token:  # token expired
                    logging.info(f'resource {resource_obj.name} is no longer belongs to token: {token}')
                    return False
            else:
                logging.info(f'resource {resource_obj.name} is no longer exists in the system, therefore token '
                             f'{token} is not valid')
                return False

        return True

    @staticmethod
    def find_all_resources_with_token(token: str, all_resources_list: ResourcesListType) -> ResourcesListType:
        tmp_list = []
        for resource in all_resources_list:
            if resource.token == token:
                tmp_list.append(resource)
        return tmp_list

    async def find_resources(self, client_req_resources_list: List[ResourcesListType]) -> ResourcesListType:
        """
        find all resources that match the client_req_list
        :param client_req_resources_list: list of resources list
        example: [[a,b,c], [a,b,c], [d,e], [f]] -> must have: one of (a or b or c) and one of (a or b or c)
        and one of (d or e) and f
        :return: list of all resources that matched the client request
        """
        out_resources_list = []
        all_resources_list = await self.redis.get_all_resources()
        for resource_group in client_req_resources_list:
            if isinstance(resource_group, Resource):
                one_resource = self.find_one_resource(resource_group, all_resources_list)
                out_resources_list.append(one_resource)
            else:
                for resource in resource_group:
                    one_resource = self.find_one_resource(resource, all_resources_list)
                    out_resources_list.append(one_resource)
        return out_resources_list
