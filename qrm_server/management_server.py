import asyncio
import logging

from aiohttp import web
from db_adapters.redis_adapter import RedisDB
from http import HTTPStatus
from qrm_server.resource_definition import Resource, resource_from_json


REMOVE_JOB = '/remove_job'
MGMT_STATUS_API = '/status'
SET_SERVER_STATUS = '/set_server_status'
REMOVE_RESOURCES = '/remove_resources'
ADD_RESOURCES = '/add_resources'
SET_RESOURCE_STATUS = '/set_resource_status'
ADD_JOB_TO_RESOURCE = '/add_job_to_resource'
REDIS_PORT = 6379


async def add_resources(request) -> web.Response:
    # add resource to the active resources list
    # expected request: ['resource_1', 'resource_2', ...]
    resources_list_request = await request.json()
    logging.info(f'got request to add the following resources to DB: {resources_list_request}')
    new_resources = await add_resources_to_db(resources_list_request)
    if not new_resources:
        return web.Response(status=HTTPStatus.OK,
                            text=f'didn\'t add any resource, check if the resource already exists\n')
    return web.Response(status=HTTPStatus.OK,
                        text=f'added the following resources: {new_resources}\n')


async def add_resources_to_db(resources_list_request) -> list:
    global redis
    all_db_resources = await redis.get_all_resources()
    new_resources = []
    for resource in resources_list_request:
        await add_single_resource_with_validation(all_db_resources, new_resources, redis, Resource(**resource))
    return new_resources


async def add_single_resource_with_validation(all_db_resources, new_resources,
                                              redis: RedisDB, resource: Resource) -> None:
    if resource in all_db_resources:
        logging.error(f'resource {resource} already in DB, ignoring it')
        return
    else:

        await redis.add_resource(resource)  # init the resource as list
        new_resources.append(resource)


async def remove_resources(request) -> web.Response:
    # remove resource from db
    global redis
    resources_list = await request.json()
    logging.info(f'got request to remove the following resources from DB: {resources_list}')
    removed_list = []
    for resource_dict in resources_list:
        await redis.remove_resource(Resource(**resource_dict))
        removed_list.append(resource_dict)
        logging.info(f'removed resource {resource_dict}')
    return web.Response(status=HTTPStatus.OK,
                        text=f'removed the following resources: {removed_list}\n')


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
                                text=f'mew server status is: {req_status}\n')
        else:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'requested status is not allowed: {req_status}\n')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'must specify the status in your request: {req_dict}\n')


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
            },
        'groups':
            {
                # token1: [{resource1: type1}, {resource2: type2}, ...],
                # token2: ...
            }
    }

    for resource in await redis.get_all_resources():
        status_dict['resources_status'][resource.name] = {}
        status_dict['resources_status'][resource.name]['status'] = await redis.get_resource_status(resource)
        status_dict['resources_status'][resource.name]['jobs'] = await redis.get_resource_jobs(resource)
        add_resource_to_token_list(resource, status_dict)
    return status_dict


def add_resource_to_token_list(resource, status_dict):
    if resource.token != '':
        try:
            status_dict['groups'][resource.token].append({resource.name: resource.type})
        except KeyError as e:  # first time this token appears
            status_dict['groups'][resource.token] = []
            status_dict['groups'][resource.token].append({resource.name: resource.type})


async def remove_job(request):
    # remove the requested job from the tasks queue
    global redis
    req_dict = await request.json()
    try:
        token = req_dict['token']
        resources_list = req_dict.get('resources')
        all_resources_dict = await redis.get_all_resources_dict()
        resources_list_obj = []
        for resource_name in resources_list:
            resource = all_resources_dict.get(resource_name)
            resources_list_obj.append(resource)
        await redis.remove_job(token, resources_list_obj)
        return web.Response(status=HTTPStatus.OK, text=f'removed job: {req_dict}\n')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: "token" is a mandatory key: {req_dict}\n')


async def set_resource_status(request):
    # to enable: {resource_name: str, status: 'active'}
    # to disable: {resource_name: str, status: 'disabled'}
    global redis
    req_dict = await request.json()
    logging.info(f'got request to change resource status: {req_dict}')
    try:
        req_status = req_dict['status']
        resource_name = req_dict['resource_name']
        all_resources_dict = await redis.get_all_resources_dict()
        resource = all_resources_dict.get(resource_name)
        if not resource:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist or status is not allowed\n')
        if await redis.set_resource_status(resource=resource, status=req_status):
            return web.Response(status=HTTPStatus.OK,
                                text=f'mew resource status is: {req_status}\n')
        else:
            return web.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR,
                                text=f'couldn\'t update resource status for: {resource_name}, check server logs')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: must specify both status and resource_name in your request: {req_dict}\n')


async def add_job_to_resource(request):
    global redis
    req_dict = await request.json()
    try:
        resource_name = req_dict['resource_name']
        all_resources_dict = await redis.get_all_resources_dict()
        resource = all_resources_dict.get(resource_name)
        job = req_dict['job']
        if await redis.add_job_to_resource(resource=resource, job=job):
            return web.Response(status=HTTPStatus.OK, text=f'added job: {job} to resource: {resource_name}\n')
        else:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist or job is not dict: {job}\n')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: must specify both job and resource_name in your request: {req_dict}\n')


def main(redis_port: int = REDIS_PORT, port: int = 8080):
    init_redis(redis_port)
    app = web.Application()
    app.add_routes([web.post(f'{ADD_RESOURCES}', add_resources),
                    web.post(f'{REMOVE_RESOURCES}', remove_resources),
                    web.post(f'{SET_SERVER_STATUS}', set_server_status),
                    web.get(f'{MGMT_STATUS_API}', status),
                    web.get(f'/', status),
                    web.post(f'{REMOVE_JOB}', remove_job),
                    web.post(f'{SET_RESOURCE_STATUS}', set_resource_status),
                    web.post(f'{ADD_JOB_TO_RESOURCE}', add_job_to_resource)])
    web.run_app(app, port=port)


def init_redis(redis_port: int = REDIS_PORT):
    global redis
    redis = RedisDB(redis_port)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()
