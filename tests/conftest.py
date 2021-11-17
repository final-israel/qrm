import asyncio
import logging
import pytest
from aiohttp import web
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server.resource_definition import Resource


REDIS_PORT = 6379

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')


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
