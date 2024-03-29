import asyncio
import copy
import datetime
import logging
from db_adapters.redis_adapter import RedisDB
from qrm_defs.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName, \
    generate_token_from_seed, ACTIVE_STATUS, DISABLED_STATUS, PENDING_STATUS
from typing import List, Dict
from abc import ABC, abstractmethod

NOT_VALID = 'not_valid'
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
    async def get_resource_req_resp(self, token: str) -> ResourcesRequestResponse:
        pass

    @abstractmethod
    async def init_backend(self) -> None:
        pass

    @abstractmethod
    async def stop_backend(self) -> None:
        pass


class QueueManagerBackEnd(QrmIfc):
    def __init__(self,
                 redis_port: int = REDIS_PORT,
                 use_pending_logic: bool = False):
        """
        :Params:
        redis_port - redis server port to connect
        use_pending_logic - qrm will remove the server to PENDING after remove the active job
        and will consider job as active only if the server change state to ACTIVE
        """
        self.redis = RedisDB(redis_port)
        self.use_pending_logic = use_pending_logic
        self.tokens_change_event = {}  # type: Dict[str, QRMEvent]
        self.lock = asyncio.Lock()

    # Recovery from DB
    async def init_backend(self) -> None:
        """
        this method handles recovery from DB, it handles db initialization
        and then init all backend data structures during DB recovery
        :return: None
        """
        await self.redis.init_default_params()
        await self.init_open_tokens_events()
        await self.init_workers_with_open_requests()

    async def init_open_tokens_events(self) -> None:
        """
        init all tokens_change_events data structure for open requests
        :return: None
        """
        logging.info('start init open tokens')
        all_tokens = await self.redis.get_all_open_tokens()
        logging.info(f'all tokens in db are {all_tokens}')
        for token in all_tokens:
            logging.info(f'init events for token {token}')
            self.tokens_change_event[token] = QRMEvent()
            self.tokens_change_event[token].set()

    async def init_workers_with_open_requests(self) -> None:
        """
        for every open request, init it's worker
        :return: None
        """
        open_requests = await self.redis.get_open_requests()
        for token in open_requests.keys():
            asyncio.ensure_future(self.names_worker(token))

    async def stop_backend(self) -> None:
        await self.redis.close()

    async def names_worker(self, token: str) -> ResourcesRequestResponse:
        """
        this is the "main" function of resources request by names.
        it handles the request end to end by reading the open request from DB,
        call other methods to find the relevant available resources and wait
        for signals until the request complete.
        :param token: request token
        :return: returns the resources response
        """

        user_req = await self.redis.get_open_request_by_token(token)
        updated_req = copy.deepcopy(user_req)

        tasks = []

        for req_index, resources_list_request in enumerate(user_req.names):
            tasks.append(
                asyncio.ensure_future(
                    self.single_resource_by_name_worker(
                        resources_list_request,
                        token,
                        updated_req,
                        req_index
                    )
                )
            )

        for task in tasks:
            ret = await task
            if isinstance(ret, ResourcesRequestResponse):
                return ret

        logging.info(f'done handling token: {token}')

        if self.use_pending_logic:
            await self.move_resources_to_pending(token=token)

        return await self.finalize_filled_request(token)

    async def single_resource_by_name_worker(
            self,
            resources_list_request: ResourcesByName,
            token: str,
            updated_req: ResourcesRequest,
            req_index: int
    ) -> ResourcesRequestResponse:

        while resources_list_request.count > 0:
            logging.info(f'remaining resources for token: {token} is: {resources_list_request.count}')
            await self.find_available_resources_by_names(resources_list_request, token)
            updated_req.names[req_index] = resources_list_request  # this DS is changed by reference
            logging.info(f'update open request for token: {token} with: {resources_list_request}')
            await self.redis.update_open_request(token, updated_req)
            if resources_list_request.count != 0:
                logging.info(f'waiting for signal on token: {token}')
                reason = await self.worker_wait_for_continue_event(token)
                logging.info(f'received signal for {token}')
                if reason == CANCELED:
                    return ResourcesRequestResponse()
                if reason == NOT_VALID:
                    logging.error(f'request {token} is not valid')
                    rrr = ResourcesRequestResponse(token=token, message='request not valid')
                    await self.redis.set_req_resp(rrr)
                    return rrr
            else:
                await self.remove_job_from_unused_resources(resources_list_request.names, token)

    async def remove_job_from_unused_resources(self, resources_names: List[str], token: str):
        """
        remove the request jobs from all unused resources,
        for example, if the request is one resource from three, and th BE found
        one resource from the three (the job is first in queue there),
        it will remove the job from the other two resources
        :param resources_names: list of resources names
        :param token: request token
        """
        logging.info(f'removing token {token} from unused resources: {resources_names}')
        for res_name in resources_names:
            resource = await self.redis.get_resource_by_name(res_name)
            await self.redis.remove_job(token, [resource])
            await self.signal_due_to_job_removal(resource)

    async def signal_due_to_job_removal(self, resource):
        active_job = await self.redis.get_active_job(resource)
        try:
            self.tokens_change_event[active_job['token']].set()
        except KeyError:
            pass

    async def finalize_filled_request(self, token: str):
        """
        request filled, now wait for resources active state,
        remove the open request and generate the filled token in DB
        :param token: request token
        :return: ResourcesRequestResponse
        """
        await self.redis.remove_open_request(token)
        response = await self.redis.get_partial_fill(token)
        logging.info(f'fill for token {token} is {response}')
        resp_for_token = await self.redis.get_req_resp_for_token(token)
        logging.info(f'resp for token {token} is {resp_for_token}')
        resources_list = await self.redis.get_resources_by_names(response.names)
        await self.wait_for_active_state_on_all_resources(token)
        await self.reorder_token_with_tags(token, resources_list)
        await self.redis.generate_token(token, resources_list)
        logging.info(f'finalize fill for token {token}')
        return response


    async def reorder_token_with_tags(self, token: str, resources_list: List[Resource]) -> None:
        # this method ensures that the response will be in the same order according to tags request
        res_list_names = QueueManagerBackEnd.get_resources_names_from_resources_list(resources_list)
        orig_request = await self.redis.get_orig_request(token)
        reordered_resources_list = []  # type: List[Resource]

        if orig_request.tags:
            for rbt in orig_request.tags:
                all_resources_for_tag = await self.redis.get_resources_names_by_tags(rbt.tags)
                res_for_tag = set(all_resources_for_tag).intersection(res_list_names)
                for resource_name in res_for_tag:
                    resource = await self.redis.get_resource_by_name(resource_name)
                    reordered_resources_list.append(resource)

            resources_list = reordered_resources_list

            await self.reorder_response_in_db(resources_list, token)

    async def reorder_response_in_db(self, resources_list, token):
        reordered_names = QueueManagerBackEnd.get_resources_names_from_resources_list(resources_list)
        rrr = await self.redis.get_req_resp_for_token(token)
        rrr.names = reordered_names
        await self.redis.set_req_resp(rrr)


    async def wait_for_active_state_on_all_resources(self, token: str) -> None:
        """
        this method is blocking until all resources for the token becomes active
        :param token: request token
        :return: None
        """
        resources_list = await self.redis.get_partial_fill(token)
        for resource_name in resources_list.names:
            resource = await self.redis.get_resource_by_name(resource_name)
            if resource.status != ACTIVE_STATUS:
                logging.info(f'waiting for active state on resource {resource.name}')
                await self.redis.wait_for_resource_active_status(resource)
                logging.info(f'done waiting for active state on resource {resource.name}')
        return

    async def worker_wait_for_continue_event(self, token: str) -> str:
        """
        wait for continue event which signal some change on the relevant token.
        once the event is set it means that one of the resources related to this
        token had some change in it's queue
        :param token: request token
        :return: None
        """
        self.tokens_change_event[token].clear()
        await self.tokens_change_event[token].wait()

        return self.tokens_change_event[token].reason

    async def find_available_resources_by_names(self, resources_list_request: ResourcesByName,
                                                token: str) -> None:
        """
        for each resource in the request, check if the active job is the
        one with the requested token and if the resource is not disabled.
        :param resources_list_request: ResourceByName, this is actually
        the one of the items in the list of requests of ResourcesRequest.names
        :param token: request token
        :param lock: lock object to lock the redis access
        :return: None, changes by reference the resources_list_request
        """

        matched_resources = []
        for resource_name in resources_list_request.names:
            async with self.lock:
                resource = await self.redis.get_resource_by_name(resource_name)
                active_job = await self.redis.get_active_job(resource)
                logging.info(f'active job for resource {resource_name} is: {active_job.get("token")}')

                if active_job.get('token') == token and \
                        resource.status != DISABLED_STATUS \
                        and resources_list_request.count > 0:
                    if resource.token:
                        await self.cancel_request(resource.token)
                    await self.redis.partial_fill_request(token, resource)
                    logging.debug(f'resource {resource.name} is now belongs to token {token}')
                    matched_resources.append(resource_name)
                    resources_list_request.count -= 1

        resources_list_request.names = \
            [res for res in resources_list_request.names
             if res not in matched_resources]

    async def generate_jobs_from_names_request(self, token: str) -> None:
        """
        just creates jobs and add them to all the resources in the open request
        :param token: request token
        :return: None
        """

        user_req = await self.redis.get_open_request_by_token(token)

        for req_by_name in user_req.names:
            for res_name in req_by_name.names:
                resource = await self.redis.get_resource_by_name(res_name)
                if resource.status != DISABLED_STATUS:
                    await self.generate_job(resource, user_req.token)
                else:
                    logging.info(f'doesn\'t add job {token} for resource {resource.name}')

    async def cancel_request(self, token: str) -> None:
        """
        cancel active request by it's token
        :param token: request token
        :return: None, just remove the request and handle resources cleanup (pending)
        """

        await self.redis.delete_token_last_update_time(token)
        await self.redis.delete_auto_managed_token(token)
        rrr = await self.redis.get_req_resp_for_token(token)
        if not rrr.is_token_active_in_queue:
            rrr.is_valid = False
            await self.redis.set_req_resp(rrr)

        affected_resources = await self.redis.remove_job(token=token)
        logging.info(f'resources {affected_resources} were affected by cancel on token {token}')

        for resource in affected_resources:
            ret = await self.redis.get_active_job(resource)
            if "token" not in ret:
                continue

            affected_token = ret["token"]
            # release coros
            self.tokens_change_event[affected_token].set()
        try:
            logging.debug(f'setting token change event for {token}')
            self.tokens_change_event[token].set(reason=CANCELED)
        except KeyError as e:
            logging.error(f'got request to cancel unknown token {token}')

    async def move_resources_to_pending(self, token: str) -> None:
        """
        if all the queues are empty, doesn't move the resources to pending,
        else, move all token active resources to pending.
        :param token: request token
        :return: None
        """

        resources_for_token = await self.redis.get_partial_fill(token)

        affected_tokens = set()

        for resource in resources_for_token.names:
            resource = await self.redis.get_resource_by_name(resource)
            affected_tokens.add(resource.token)

        logging.info(f'affected tokens by new request on token {token} are {affected_tokens}')
        logging.info(f'will move all resources of tokens {affected_tokens} to pending')

        for affected_token in affected_tokens:
            await self.move_all_token_resources_to_pending(affected_token)
            await self.redis.destroy_token(affected_token)

    async def move_all_token_resources_to_pending(self, token: str) -> None:
        """
        move all resources with this active token to PENDING
        :param token: request token
        :return: None
        """

        if not await self.redis.get_token_resources(token):
            logging.info(f'token {token} already destroyed, won\'t move it\'s resources to pending')
            return

        resources_for_token = await self.redis.get_partial_fill(token)
        logging.info(f'will move resources: {resources_for_token} to PENDING state')

        for resource_name in resources_for_token.names:
            resource = await self.redis.get_resource_by_name(resource_name)
            if resource.status != DISABLED_STATUS:
                await self.redis.set_resource_status(resource, PENDING_STATUS)

    async def is_more_than_one_job_waiting_in_queue(self, resource) -> bool:
        """
        the queue always has empty job due to redis implementation,
        so we must check if the queue depth larger than two
        :param resource: Resource object
        :return: True if there is more than one job in queue, else False
        """

        return len(await self.redis.get_resource_jobs(resource)) > 2

    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        requested_token = resources_request.token
        all_resources_dict = await self.redis.get_all_resources_dict()
        resources_token_list = await self.redis.get_token_resources(requested_token)
        if self.is_token_valid(requested_token, all_resources_dict, resources_token_list):
            await self.redis.set_active_token_for_user_token(
                requested_token, requested_token
            )
            return await self.handle_token_request_for_valid_token(requested_token, resources_token_list)

        if await self.is_request_active(token=resources_request.token):
            await self.redis.set_active_token_for_user_token(
                user_token=resources_request.token,
                active_token=resources_request.token
            )

            return ResourcesRequestResponse(
                token=resources_request.token,
                message='request in progress'
            )

        active_token = generate_token_from_seed(requested_token)
        resources_request.token = active_token
        await self.redis.set_active_token_for_user_token(
            requested_token, active_token
        )

        if resources_request.auto_managed:
            await self.redis.add_auto_managed_token(active_token)

        await self.update_last_token_req_time(active_token)

        await self.init_event_for_token(active_token)

        await self.redis.save_orig_resources_req(resources_request)

        await self.convert_tags_to_names(resources_request)

        if not await self.validate_new_request(resources_request):
            return ResourcesRequestResponse(is_valid=False)

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

    async def convert_tags_to_names(self, resources_req: ResourcesRequest) -> List[str]:
        """
        this method converts tags for resources names and change the resource_req by reference
        for each tag, it finds the resources that has this tag and add the correspond names request
        :param resources_req: user resources request
        """
        for rbt in resources_req.tags:
            resources_names = await self.redis.get_resources_names_by_tags(rbt.tags)
            if not resources_names:  # no matched resources for tag
                return []
            resources_req.add_request_by_names(names=resources_names, count=rbt.count)

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

    async def handle_token_request_for_valid_token(self, token: str, resources_token_list: List[Resource]) \
            -> ResourcesRequestResponse:
        # this method assumes that the token is valid and all resources are available with this token
        resources_request_resp = ResourcesRequestResponse()
        resources_request_resp.token = token
        for resource in resources_token_list:
            active_job = await self.redis.get_active_job(resource)
            if not active_job:
                await self.generate_job(resource, token)
            resources_request_resp.names.append(resource.name)
        await self.update_last_token_req_time(token)
        return resources_request_resp

    async def generate_job(self, resource, token):
        logging.info(f'add job {token} for resource {resource}')
        await self.redis.add_job_to_resource(resource, {'token': token})

    async def is_request_active(self, token: str) -> bool:
        # request is active if it's not filled, or it's already cancelled:
        try:
            is_filled = await self.redis.is_request_filled(token)
            is_cancelled = self.tokens_change_event[token].reason == CANCELED
            if not is_cancelled:  # don't update last_seen for cancelled tokens
                await self.update_last_token_req_time(token)
            is_not_valid = self.tokens_change_event[token].reason == NOT_VALID
            logging.info(f'request for token: {token} cancelled: {is_cancelled}, '
                         f'filled: {is_filled}, not_valid: {is_not_valid}')
            return not (is_filled or is_cancelled or is_not_valid)
        except KeyError as e:
            logging.info(f'got request for unknown token {token}')
            rrr = ResourcesRequestResponse()
            rrr.token = token
            rrr.is_valid = False
            rrr.message = f'unknown token in qrm {token}'
            await self.redis.set_req_resp(rrr)
            return False

    async def update_last_token_req_time(self, token: str) -> None:
        request_time = datetime.datetime.now().strftime('%m/%d/%Y, %H:%M:%S')
        await self.redis.update_token_last_update_time(token, last_update=request_time)

    async def get_new_token(self, token: str) -> str:
        new_token = await self.redis.get_active_token_from_user_token(token)
        logging_count = 0
        while not new_token:
            await asyncio.sleep(0.1)
            if logging_count % 10 == 0:  # log only every 10 iterations
                logging.info(f'waiting for new token on requested token {token}')
            new_token = await self.redis.get_active_token_from_user_token(token)
            logging_count += 1
        return new_token

    async def get_resource_req_resp(self, token: str) -> ResourcesRequestResponse:
        # if the request is not totally filled, you will get the current partial fill.
        # in case you want only totally filled, first check is_request_active method
        rrr = await self.redis.get_req_resp_for_token(token)
        if not rrr.names:
            return rrr
        for resource_name in rrr.names:
            resource = await self.redis.get_resource_by_name(resource_name)
            res_job = await self.redis.get_active_job(resource)
            all_resources_dict = await self.redis.get_all_resources_dict()
            resources_token_list = await self.redis.get_token_resources(token)

            if self.is_token_valid(token=token,
                                   resources_dict=all_resources_dict,
                                   original_resources_token_list=resources_token_list):
                rrr.is_valid = True

            if res_job.get('token') != token:
                rrr.is_token_active_in_queue = False
                return rrr
        rrr.is_token_active_in_queue = True  # all resources jobs are active in queues
        return rrr

    async def validate_new_request(self, resources_request: ResourcesRequest) -> bool:
        all_validations = list()
        msg = ''
        all_validations.extend([
            await self.validate_enough_resources(resources_request),
        ])
        for ret in all_validations:
            if ret != '':
                msg += f'{ret},  '
        if msg:
            rrr = ResourcesRequestResponse(
                token=resources_request.token,
                message=msg,
                is_valid=False
            )
            await self.redis.set_req_resp(rrr)
            logging.error(f'request for token {resources_request.token} is not valid: {msg}')
            self.tokens_change_event[resources_request.token].set()
            self.tokens_change_event[resources_request.token].reason = NOT_VALID
            return False
        return True

    async def validate_enough_resources(self, resources_request) -> str:
        ret_str = ''
        req_not_empty = QueueManagerBackEnd.validate_request_not_empty(resources_request)
        if req_not_empty:
            ret_str += f'{req_not_empty}, '
        for names_request in resources_request.names:
            available_res = 0  # resources from request not in disables state
            for res_name in names_request.names:
                resource = await self.redis.get_resource_by_name(res_name)
                if not resource:  # resource not in DB
                    ret_str += f'WARN: resource {res_name} does not exist in DB, '
                    continue
                if resource.status != DISABLED_STATUS:
                    available_res += 1
            if available_res < names_request.count:
                logging.error(f'not enough available resources for {resources_request}')
                ret_str += f'not enough available resources for {resources_request}, '
        return ret_str

    @staticmethod
    def validate_request_not_empty(resource_request: ResourcesRequest) -> str:
        if resource_request.tags and not resource_request.names:  # this means that tags didn't matched any resources
            return f'no matched resources for tags: {resource_request.tags}'
        if not resource_request.names:
            return 'request doesn\'t contains names and tags'''
        return ''

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

        logging.info(f'token {token} is still valid')
        return True

    @staticmethod
    def get_resources_names_from_resources_list(resources: List[Resource]) -> List[str]:
        res_names_list = []
        for res in resources:
            res_names_list.append(res.name)
        return res_names_list