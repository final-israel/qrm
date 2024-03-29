import argparse
import logging
import datetime
import sys
from logging.handlers import TimedRotatingFileHandler
from aiohttp import web
from db_adapters.redis_adapter import RedisDB
from http import HTTPStatus
from qrm_defs.qrm_urls import MGMT_STATUS_API, SET_SERVER_STATUS, REMOVE_RESOURCES, ADD_RESOURCES, \
    SET_RESOURCE_STATUS, ADD_TAG_TO_RESOURCE, REMOVE_TAG_FROM_RESOURCE
from qrm_defs.resource_definition import Resource
from pathlib import Path

AUTO_MANAGED_TOKENS = 'auto_managed_tokens'

LAST_UPDATE_TIME = 'token_last_update_time'
LISTEN_PORT = 8080

REDIS_PORT = 6379

LOG_FILE_PATH = '/tmp/log/qrm-server/qrm_mgmt_server.txt'
VERSION_FILE_NAME = 'qrm_mgmt_server_ver.yaml'
here = Path(__file__).resolve().parent

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
                #       type: str
                #       active_job: token1
                #       jobs: [
                #           {}, # first in queue
                #           {}  # last in queue
                #       ]
                #   }
            },
        'tokens_resources_group':
            {
                # token1: {
                #   type1: [res1, res2],
                #   type2: [res3]
                #   }
                # token2: ...
            },
        LAST_UPDATE_TIME:
            await redis.get_all_tokens_last_update(),

        AUTO_MANAGED_TOKENS:
            await redis.get_all_auto_managed_tokens()
    }

    for resource in await redis.get_all_resources():
        jobs = await redis.get_resource_jobs(resource)
        jobs.remove({})
        jobs.reverse()
        status_dict['resources_status'][resource.name] = {}
        status_dict['resources_status'][resource.name]['status'] = await redis.get_resource_status(resource)
        status_dict['resources_status'][resource.name]['type'] = await redis.get_resource_type(resource)
        status_dict['resources_status'][resource.name]['active_job'] = await redis.get_active_job(resource)
        status_dict['resources_status'][resource.name]['jobs'] = jobs
        status_dict['resources_status'][resource.name]['tags'] = resource.tags
        add_resource_to_token_list(resource, status_dict)


    return status_dict


def add_resource_to_token_list(resource, status_dict):
    if resource.token != '':
        try:
            status_dict['tokens_resources_group'][resource.token]
        except KeyError as e:
            status_dict['tokens_resources_group'][resource.token] = {}
        try:
            status_dict['tokens_resources_group'][resource.token][resource.type]
        except KeyError as e:
            status_dict['tokens_resources_group'][resource.token][resource.type] = []
        status_dict['tokens_resources_group'][resource.token][resource.type].append(resource.name)


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
                                text=f'new resource status is: {req_status}\n')
        else:
            return web.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR,
                                text=f'couldn\'t update resource status for: {resource_name}, check server logs')
    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: must specify both status and resource_name in your request: {req_dict}\n')


async def add_tag_to_resource(request):
    global redis
    req_dict = await request.json()
    try:
        resource_name = req_dict['resource_name']
        all_resources_dict = await redis.get_all_resources_dict()
        resource = all_resources_dict.get(resource_name)

        if not resource:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist\n')

        tag = req_dict.get('tag')

        if not tag:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: tag is not specified\n')

        if await redis.add_tag_to_resource(resource=resource, tag=tag):
            return web.Response(status=HTTPStatus.OK, text=f'added tag: {tag} to resource: {resource_name}\n')
        else:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist or tag is not dict: {tag}\n')

    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: must specify both tag and resource_name in your request: {req_dict}\n')


async def remove_tag_from_resource(request):
    global redis
    req_dict = await request.json()
    try:
        resource_name = req_dict['resource_name']
        all_resources_dict = await redis.get_all_resources_dict()
        resource = all_resources_dict.get(resource_name)

        if not resource:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist\n')

        tag = req_dict.get('tag')

        if not tag:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: tag is not specified\n')

        if await redis.remove_tag_from_resource(resource=resource, tag=tag):
            return web.Response(status=HTTPStatus.OK, text=f'removed tag: {tag} from resource: {resource_name}\n')
        else:
            return web.Response(status=HTTPStatus.BAD_REQUEST,
                                text=f'Error: resource {resource_name} does not exist or tag is not dict: {tag}\n')

    except KeyError as e:
        return web.Response(status=HTTPStatus.BAD_REQUEST,
                            text=f'Error: must specify both tag and resource_name in your request: {req_dict}\n')


def config_log(path_to_log_file: str = LOG_FILE_PATH, loglevel=None):
    print(f'log file path is: {path_to_log_file}')
    if loglevel is None:
        loglevel = logging.INFO
    Path(path_to_log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=loglevel,
                        format='[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
    handler = TimedRotatingFileHandler(path_to_log_file, when='midnight', backupCount=365,
                                       encoding='utf-8')
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.info('************************************************************************************')
    logging.info(f'new server started at: {str(datetime.datetime.now())}')
    logging.info(f'log file path is: {path_to_log_file}')


def get_version_str() -> str:
    try:
        file_path = f'{here}/{VERSION_FILE_NAME}'
        logging.info(f'open file at : {file_path}')
        with open(file_path, 'r') as fid:
            version_str = ''.join(fid.readlines())
        return version_str
    except FileNotFoundError:
        file_path = f'{here}/_{VERSION_FILE_NAME}'
        logging.info(f'open file at : {file_path}')
        with open(file_path, 'r') as fid:
            version_str = ''.join(fid.readlines())
        return version_str


def full_version_str() -> str:
    return '\nthe app version is:\n' + get_version_str()


def print_version_str():
    logging.info(full_version_str())


def main(redis_port: int = REDIS_PORT, listen_port: int = LISTEN_PORT, path_to_log_file: str = LOG_FILE_PATH,
         loglevel: int = None):
    if loglevel is None:
        loglevel = logging.INFO
    config_log(path_to_log_file=path_to_log_file, loglevel=loglevel)
    print_version_str()
    init_redis(redis_port)
    app = web.Application()
    app.add_routes([web.post(f'{ADD_RESOURCES}', add_resources),
                    web.post(f'{REMOVE_RESOURCES}', remove_resources),
                    web.post(f'{SET_SERVER_STATUS}', set_server_status),
                    web.get(f'{MGMT_STATUS_API}', status),
                    web.get(f'/', status),
                    web.post(f'{SET_RESOURCE_STATUS}', set_resource_status),
                    web.post(f'{ADD_TAG_TO_RESOURCE}', add_tag_to_resource),
                    web.post(f'{REMOVE_TAG_FROM_RESOURCE}', remove_tag_from_resource)])
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
    parser.add_argument('-d', '--debug',
                        help="Print lots of debugging statements",
                        action="store_const", dest="loglevel", const=logging.DEBUG,
                        default=logging.INFO)
    parser.add_argument('--version',
                        action='store_true',
                        default=False,
                        help='print version and exit')
    args = parser.parse_args()
    if args.version:
        print(full_version_str())
        sys.exit(0)
    return args


def init_redis(redis_port: int = REDIS_PORT):
    global redis
    redis = RedisDB(redis_port)


async def close_redis(request):
    global redis
    await redis.close()


if __name__ == '__main__':
    try:
        args = create_parser()
        main(redis_port=args.redis_port,
             listen_port=args.listen_port,
             path_to_log_file=args.log_file_path,
             loglevel=args.loglevel)
    except KeyboardInterrupt:
        print('\n\nProgram terminated by user. Exiting...')
        try:
            logging.info('\n\nProgram terminated by user. Exiting...')
        except Exception:
            pass
