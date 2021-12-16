import aioredis
import asyncio
import json
import logging
from redis_adapter import RedisDB
from qrm_server import resource_definition
from typing import Dict, List

REDIS_PORT = 6379


class QueueManagerBackEnd(object):
    def __init__(self, redis_port: int = REDIS_PORT):
        self.redis: RedisDB = None
        if redis_port:
            self.redis = RedisDB(redis_port)

    def find_one_resource(self, resource: resource_definition.Resource, all_resources_dict:Dict['str', resource_definition.Resource]) \
            -> resource_definition.Resource:
        dict_of_resources_with_token = self.find_all_resources_with_token(resource.token, all_resources_dict)
        if len(dict_of_resources_with_token) == 1:
            for one_resource in dict_of_resources_with_token.values():
                return one_resource
        elif len(dict_of_resources_with_token) == 0:
            return []
        else:
            raise NotImplemented


    @staticmethod
    def find_all_resources_with_token(token: str, all_resources_dict: Dict['str', resource_definition.Resource]) -> \
            Dict[str,resource_definition.Resource]:
        tmp_dict = {}
        for k, v in all_resources_dict.items():
            if v.token == token:
                tmp_dict[k] = v
        return tmp_dict


    @staticmethod
    def get_resource(self, resource_group: resource_definition.Resource or tuple) -> resource_definition.Resource:
        if isinstance(resource_group, resource_definition.Resource):
            yield resource_group
        else:
            for resource in resource_group:
                yield resource

    async def _get_all_obj_resources_dict_from_db(self) -> Dict[str, resource_definition.Resource]:
        all_resources_dict = await self.redis.get_all_resources_dict()
        for k, v in all_resources_dict.items():
            all_resources_dict[k] = resource_definition.load_from_json(v)
        return all_resources_dict

    async def find_resources(self, client_req_resources_list) -> List[resource_definition.Resource]:
        all_resources_dict = await self._get_all_obj_resources_dict_from_db()
        out_resources_list = []
        for resource_group in client_req_resources_list:
            if isinstance(resource_group, resource_definition.Resource):
                one_resource = self.find_one_resource(resource_group, all_resources_dict)
                out_resources_list.append(one_resource)
            else:
                for resource in resource_group:
                    one_resource = self.find_one_resource(resource, all_resources_dict)
                    out_resources_list.append(one_resource)
        return out_resources_list





        for resource_obj in client_req_resources_list:
            return all_resources_dict


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
