import asyncio
import json
import logging
from redis_adapter import RedisDB
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName
from typing import List, Dict

REDIS_PORT = 6379
ResourcesListType = List[Resource]


class QueueManagerBackEnd(object):
    def __init__(self, redis_port: int = REDIS_PORT):
        if redis_port:
            self.redis = RedisDB(redis_port)
        else:
            self.redis = RedisDB(REDIS_PORT)

    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        token = resources_request.token
        all_resources_dict = await self.redis.get_all_resources_dict()
        resources_token_list = await self.redis.get_token_resources(token)
        if self.is_token_valid(token, all_resources_dict, resources_token_list):
            return await self.handle_token_request_for_valid_token(token, resources_token_list)
        elif resources_request.names:
            # TODO: generate new token for this job, since this token expired. but don't override the old token since
            #  it's used to give priority to resources with the old token
            await self.reorder_names_request(token, resources_request.names, all_resources_dict)

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
        await self.redis.add_job_to_resource(resource, {'job_id': token})

    @staticmethod
    def is_token_valid(token: str, resources_dict: Dict[str, Resource],
                       original_resources_token_list: List[Resource]) -> bool:
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


class QueueManager(asyncio.Protocol):
    def __init__(self, ):
        self.transport = None
        self.client_name = ''  # type: str
        self.loop = asyncio.get_running_loop()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        peer_name = transport.get_extra_info('peername')
        logging.info('Connection from {}'.format(peer_name))
        self.transport = transport

    def data_received(self, data) -> None:
        message = data.decode()
        try:
            message_dict = json.loads(message)
            logging.debug('Data received: {!r}'.format(message_dict))
        except json.JSONDecodeError as exc:
            logging.error(f'can\'t convert message to json: {message}\n{exc}')

        logging.info('Send: {!r}'.format(message))
        self.transport.write(data)

    def connection_lost(self, exc: Exception or None) -> None:
        logging.info(f'connection closed: {exc}')
        self.transport.close()

    def get_resources(self, resources_list: list) -> list:
        return []


async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: QueueManager(),
        '127.0.0.1', 8888)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
