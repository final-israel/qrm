import asyncio
import logging
import pytest
from aiohttp import web
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server


REDIS_PORT = 6379

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')


@pytest.fixture(scope='function')
def redis_db_object(redis_my) -> redis_adapter.RedisDB:
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT)
    yield test_adapter_obj
    del test_adapter_obj


@pytest.fixture
def post_to_mgmt_server(loop, aiohttp_client):
    app = web.Application()
    management_server.init_redis()
    app.router.add_post(management_server.ADD_RESOURCES, management_server.add_resources)
    app.router.add_post(management_server.REMOVE_RESOURCES, management_server.remove_resources)
    app.router.add_get(management_server.STATUS, management_server.status)
    app.router.add_post(management_server.SET_SERVER_STATUS, management_server.set_server_status)
    return loop.run_until_complete(aiohttp_client(app))
