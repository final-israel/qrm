import argparse
import logging

from aiohttp import web
from db_adapters.redis_adapter import RedisDB
from http import HTTPStatus
from qrm_defs.qrm_urls import REMOVE_JOB, MGMT_STATUS_API, SET_SERVER_STATUS, REMOVE_RESOURCES, ADD_RESOURCES, \
    SET_RESOURCE_STATUS, ADD_JOB_TO_RESOURCE
from qrm_defs.resource_definition import Resource
from pathlib import Path
LISTEN_PORT = 8080

REDIS_PORT = 6379

LOG_FILE_PATH = '/tmp/log/qrm-server/qrm_server.txt'


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
                #       tags: [],
                #   }
            },
        'tokens_resources_group':
            {
                # token1: [{resource1: type1}, {resource2: type2}, ...],
                # token2: ...
            }
    }

    for resource in await redis.get_all_resources():
        status_dict['resources_status'][resource.name] = {}
        status_dict['resources_status'][resource.name]['status'] = await redis.get_resource_status(resource)
        status_dict['resources_status'][resource.name]['jobs'] = await redis.get_resource_jobs(resource)
        status_dict['resources_status'][resource.name]['tags'] = resource.tags
        add_resource_to_token_list(resource, status_dict)
    return status_dict


def add_resource_to_token_list(resource, status_dict):
    if resource.token != '':
        try:
            status_dict['tokens_resources_group'][resource.token].append({resource.name: resource.type})
        except KeyError as e:  # first time this token appears
            status_dict['tokens_resources_group'][resource.token] = []
            status_dict['tokens_resources_group'][resource.token].append({resource.name: resource.type})


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


def config_log(path_to_log_file: str = LOG_FILE_PATH):
    print(f'log file path is: {path_to_log_file}')
    Path(path_to_log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=path_to_log_file, level=logging.DEBUG, format=
    '[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
    logging.info(f'log file path is: {path_to_log_file}')


def main(redis_port: int = REDIS_PORT, listen_port: int = LISTEN_PORT, path_to_log_file: str = LOG_FILE_PATH ):
    config_log(path_to_log_file=path_to_log_file)
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
    app.on_shutdown.append(close_redis)
    web.run_app(app, port=listen_port)


def create_parser() -> argparse.ArgumentParser.parse_args:
    parser = argparse.ArgumentParser(description='QRM HTTP SERVER')
    parser.add_argument('--redis_port',
                        help='redis server listen port',
                        default=REDIS_PORT)
    parser.add_argument('--listen_port',
                        help='http listen port',
                        default=LISTEN_PORT)
    parser.add_argument('--log_file_path',
                        help='path to text log file',
                        default=LOG_FILE_PATH)
    return parser.parse_args()


def init_redis(redis_port: int = REDIS_PORT):
    global redis
    redis = RedisDB(redis_port, name='mgmt_sevrer_redis')


async def close_redis(request):
    global redis
    await redis.close()


if __name__ == '__main__':
    args = create_parser()
    main(args.redis_port, args.listen_port, args.log_file_path)
