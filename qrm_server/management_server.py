import aioredis
import logging


from aiohttp import web

redis = aioredis.from_url(
            "redis://localhost", encoding="utf-8", decode_responses=True
        )


async def add_resources(request):
    # add resource to the active resources list
    # expected request: ['resource_1', 'resource_2', ...]
    resources_list = await request.json()
    logging.info(f'got request to add the following resources to DB: {resources_list}')
    all_db_vars = []
    new_resources = []
    async for key in redis.scan_iter():
        all_db_vars.append(key)
    for resource_name in resources_list:
        if resource_name in all_db_vars:
            logging.error(f'resource {resource_name} already in DB, ignoring it')
            continue
        await redis.rpush(resource_name, '')  # init the resource as list
        await redis.rpush('resources_list', resource_name)
        new_resources.append(resource_name)
    return web.Response(text=f'added the following resources: {new_resources}')


async def remove_resource(request):
    # remove resource from db
    req_dict = await request.json()
    pass


async def disable_server(request):
    # control the disable \ enable server bit
    req_dict = await request.json()
    pass


async def status(request):
    # (get) return json of all resources and their tasks queue
    data = {'foo': await redis.get('foo')}
    return web.json_response(data)


async def remove_job(request):
    # remove the requested job from the tasks queue
    req_dict = await request.json()
    pass


def main():
    app = web.Application()
    app.add_routes([web.post('/add_resources', add_resources),
                    web.post('/remove_resource', remove_resource),
                    web.post('/disable_server', disable_server),
                    web.get('/status', status),
                    web.post('/remove_job', remove_job)])
    web.run_app(app)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
