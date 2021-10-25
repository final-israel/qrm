import asyncio
import logging

from aiohttp import web
from db_adapters.redis_adapter import RedisDB
from http import HTTPStatus
from qrm_db import RESOURCE_NAME_PREFIX


REMOVE_JOB = '/remove_job'
STATUS = '/status'
SET_SERVER_STATUS = '/set_server_status'
REMOVE_RESOURCES = '/remove_resources'
ADD_RESOURCES = '/add_resources'
REDIS_PORT = 6379


async def add_resources(request) -> web.Response:
    # add resource to the active resources list
    # expected request: ['resource_1', 'resource_2', ...]
    resources_list_request = await request.json()
    logging.info(f'got request to add the following resources to DB: {resources_list_request}')
    new_resources = await add_resources_to_db(resources_list_request)
    return web.Response(status=HTTPStatus.OK,
                        reason=f'added the following resources: {new_resources}\n')


async def add_resources_to_db(resources_list_request) -> list:
    global redis
    all_db_resources = await redis.get_all_resources()
    new_resources = []
    for resource_name in resources_list_request:
        await add_single_resource_with_validation(all_db_resources, new_resources, redis, resource_name)
    return new_resources


async def add_single_resource_with_validation(all_db_resources, new_resources, redis, resource_name) -> None:
    if resource_name in all_db_resources:
        logging.error(f'resource {resource_name} already in DB, ignoring it')
    else:
        await redis.add_resource(resource_name)  # init the resource as list
        new_resources.append(resource_name)


async def remove_resources(request) -> web.Response:
    # remove resource from db
    global redis
    resources_list = await request.json()
    logging.info(f'got request to remove the following resources from DB: {resources_list}')
    removed_list = []
    for resource_name in resources_list:
        await redis.remove_resource(resource_name)
        removed_list.append(resource_name)
        logging.info(f'removed resource {resource_name}')
    return web.Response(status=HTTPStatus.OK,
                        reason=f'removed the following resources: {removed_list}\n')


async def set_server_status(request) -> web.Response:
    # to enable: {status: 'active'}
    # to disable: {status: 'disabled'}
    global redis
    req_dict = await request.json()
    logging.info(f'got request to change server status: {req_dict}')
    try:
        req_status = req_dict['status']
        if await redis.set_qrm_status(req_status):
            return web.Response(status=HTTPStatus.OK,
                                reason=f'mew server status is: {req_status}\n')
        else:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                reason=f'requested status is not allowed: {req_status}\n')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            reason=f'must specify the status in your request: {req_dict}\n')


async def status(request):
    # (get) return json of all resources and their tasks queue
    status_dict = await build_status_dict()
    return web.json_response(status_dict)


async def build_status_dict():
    global redis
    status_dict = {
        'qrm_server_status': await redis.get_qrm_status(),
        'resources_status':
            {
                # resource_1:
                #   {
                #       status: str,
                #       jobs: [
                #           {},
                #           {}
                #       ]
                #   }
            }
    }
    for resource in await redis.get_all_resources():
        resource_name = resource.split(f'{RESOURCE_NAME_PREFIX}_')[-1]
        status_dict['resources_status'][resource_name] = {}
        status_dict['resources_status'][resource_name]['status'] = \
            await redis.get_resource_status(resource_name)
        status_dict['resources_status'][resource_name]['jobs'] = \
            await redis.get_resource_jobs(resource_name=resource_name)
    return status_dict


async def remove_job(request):
    # remove the requested job from the tasks queue
    global redis
    req_dict = await request.json()
    pass


def main(redis_port: int = REDIS_PORT):
    init_redis(redis_port)
    app = web.Application()
    app.add_routes([web.post(f'{ADD_RESOURCES}', add_resources),
                    web.post(f'{REMOVE_RESOURCES}', remove_resources),
                    web.post(f'{SET_SERVER_STATUS}', set_server_status),
                    web.get(f'{STATUS}', status),
                    web.post(f'{REMOVE_JOB}', remove_job)])
    web.run_app(app)


def init_redis(redis_port: int = REDIS_PORT):
    global redis
    redis = RedisDB(redis_port)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()
