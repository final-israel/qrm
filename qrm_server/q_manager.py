import asyncio
import json
import logging
from redis_adapter import RedisDB
from qrm_server.resource_definition import Resource
from typing import Dict, List

REDIS_PORT = 6379


class QueueManagerBackEnd(object):
    def __init__(self, redis_port: int = REDIS_PORT):
        if redis_port:
            self.redis = RedisDB(redis_port)
        else:
            self.redis = RedisDB(REDIS_PORT)

    def find_one_resource(self, resource: Resource, all_resources_list: List[Resource]) -> Resource:
        list_of_resources_with_token = self.find_all_resources_with_token(resource.token, all_resources_list)
        if len(list_of_resources_with_token) == 1:
            for one_resource in list_of_resources_with_token:
                return one_resource
        elif len(list_of_resources_with_token) == 0:
            return []
        else:
            raise NotImplemented

    @staticmethod
    def find_all_resources_with_token(token: str, all_resources_list: List[Resource]) -> List[Resource]:
        tmp_list = []
        for resource in all_resources_list:
            if resource.token == token:
                tmp_list.append(resource)
        return tmp_list

    async def find_resources(self, client_req_resources_list) -> List[Resource]:
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
