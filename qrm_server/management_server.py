import logging
from db_adapters.redis_adapter import RedisDB
from aiohttp import web


REMOVE_JOB = '/remove_job'
STATUS = '/status'
DISABLE_SERVER = '/disable_server'
REMOVE_RESOURCES = '/remove_resources'
ADD_RESOURCES = '/add_resources'
REDIS_PORT = 6379


async def add_resources(request):
    # add resource to the active resources list
    # expected request: ['resource_1', 'resource_2', ...]
    resources_list_request = await request.json()
    logging.info(f'got request to add the following resources to DB: {resources_list_request}')
    new_resources = await add_resources_to_db(resources_list_request)
    return web.Response(text=f'added the following resources: {new_resources}\n')


async def add_resources_to_db(resources_list_request) -> list:
    global redis
    all_db_resources = await redis.get_all_resources()
    new_resources = []
    for resource_name in resources_list_request:
        await add_single_resource_with_validation(all_db_resources, new_resources, redis, resource_name)
    return new_resources


async def add_single_resource_with_validation(all_db_resources, new_resources, redis, resource_name):
    if resource_name in all_db_resources:
        logging.error(f'resource {resource_name} already in DB, ignoring it')
    else:
        await redis.add_resource(resource_name)  # init the resource as list
        new_resources.append(resource_name)


async def remove_resources(request):
    # remove resource from db
    global redis
    resources_list = await request.json()
    logging.info(f'got request to remove the following resources from DB: {resources_list}')
    removed_list = []
    for resource_name in resources_list:
        await redis.remove_resource(resource_name)
        removed_list.append(resource_name)
        logging.info(f'removed resource {resource_name}')
    return web.Response(text=f'removed the following resources: {removed_list}\n')


async def disable_server(request):
    # control the disable \ enable server bit
    global redis
    req_dict = await request.json()
    pass


async def status(request):
    # (get) return json of all resources and their tasks queue
    status_dict = await build_status_dict()
    return web.json_response(status_dict)


async def build_status_dict():
    global redis
    status_dict = {
        'resources_status':
            {
                # resource_1:
                #   {
                #       status: bool,
                #       jobs: []
                #   }
            }
    }
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
                    web.post(f'{DISABLE_SERVER}', disable_server),
                    web.get(f'{STATUS}', status),
                    web.post(f'{REMOVE_JOB}', remove_job)])
    web.run_app(app)


def init_redis(redis_port: int = REDIS_PORT):
    global redis
    redis = RedisDB(redis_port)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()
