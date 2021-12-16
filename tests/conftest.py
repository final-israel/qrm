import logging
import pytest
from aiohttp import web
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server.resource_definition import Resource
from qrm_server.q_manager import QueueManager, main

REDIS_PORT = 6379

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')


@pytest.fixture(scope='session')
def resource_dict_1() -> dict:
    return {'name': 'resource_1', 'type': 'server'}


@pytest.fixture(scope='session')
def resource_dict_2() -> dict:
    return {'name': 'resource_2', 'type': 'server'}


@pytest.fixture(scope='session')
def resource_dict_3() -> dict:
    return {'name': 'resource_3', 'type': 'server'}


@pytest.fixture(scope='session')
def resource_foo() -> Resource:
    return Resource(name='foo', type='server')


@pytest.fixture(scope='session')
def resource_bar() -> Resource:
    return Resource(name='bar', type='server')


@pytest.fixture(scope='function')
def redis_db_object(redis_my) -> redis_adapter.RedisDB:
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT)
    test_adapter_obj.init_params_blocking()
    yield test_adapter_obj
    del test_adapter_obj


@pytest.fixture(scope='function')
def post_to_mgmt_server(loop, aiohttp_client):
    app = web.Application(loop=loop)
    management_server.init_redis()
    app.router.add_post(management_server.ADD_RESOURCES, management_server.add_resources)
    app.router.add_post(management_server.REMOVE_RESOURCES, management_server.remove_resources)
    app.router.add_get(management_server.STATUS, management_server.status)
    app.router.add_post(management_server.SET_SERVER_STATUS, management_server.set_server_status)
    app.router.add_post(management_server.SET_RESOURCE_STATUS, management_server.set_resource_status)
    app.router.add_post(management_server.ADD_JOB_TO_RESOURCE, management_server.add_job_to_resource)
    app.router.add_post(management_server.REMOVE_JOB, management_server.remove_job)
    yield loop.run_until_complete(aiohttp_client(app))


@pytest.fixture()
def q_manager_for_test(event_loop, unused_tcp_port):
    print(f'##### ron fixture {unused_tcp_port}')
    cancel_handle = asyncio.ensure_future(main(unused_tcp_port), loop=event_loop)
    event_loop.run_until_complete(asyncio.sleep(0.01))

    try:
        print(f'##### ron b4 yield')
        yield unused_tcp_port
    finally:
        cancel_handle.cancel()

'''
@pytest.fixture(scope='function')
async def q_manager_for_test(loop, aiohttp_client):
    server = await loop.create_server(
            lambda: QueueManager(),
        '127.0.0.1', 8888)

    async with server:
        await server.serve_forever()
'''