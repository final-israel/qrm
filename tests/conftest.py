import asyncio
import logging
import pytest
from aiohttp import web
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server import qrm_http_server
from qrm_server.resource_definition import Resource
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc, \
    ResourcesRequest, ResourcesRequestResponse

REDIS_PORT = 6379

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')


# noinspection PyMethodMayBeStatic
class QueueManagerBackEndMock(QrmIfc):
    for_test_is_request_active: bool = False

    async def cancel_request(self, user_token: str) -> None:
        print('#######  using cancel_request in QueueManagerBackEndMock ####### ')
        return

    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        resources_request_res = ResourcesRequestResponse
        resources_request_res.token = resources_request.token
        return resources_request_res

    async def is_request_active(self, token: str) -> bool:
        return self.for_test_is_request_active

    async def get_new_token(self, token: str) -> str:
        return f'{token}_new'

    async def get_filled_request(self, token: str) -> ResourcesRequestResponse:
        pass


@pytest.fixture(scope='session')
def qrm_backend_mock() -> QueueManagerBackEndMock:
    return QueueManagerBackEndMock()


@pytest.fixture(scope='function')
def qrm_backend_mock_cls() -> QueueManagerBackEndMock:
    return QueueManagerBackEndMock

@pytest.fixture(scope='session')
def resource_dict_1() -> dict:
    return {'name': 'resource_1', 'type': 'server'}


@pytest.fixture(scope='session')
def resource_dict_2() -> dict:
    return {'name': 'resource_2', 'type': 'server'}


@pytest.fixture(scope='session')
def resource_dict_3() -> dict:
    return {'name': 'resource_3', 'type': 'server'}


@pytest.fixture(scope='function')
def resource_foo() -> Resource:
    return Resource(name='foo', type='server')


@pytest.fixture(scope='function')
def resource_bar() -> Resource:
    return Resource(name='bar', type='server')


@pytest.fixture(scope='function')
def redis_db_object(redis_my) -> redis_adapter.RedisDB:
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT)
    test_adapter_obj.init_params_blocking()
    yield test_adapter_obj
    del test_adapter_obj


@pytest.fixture(scope='function')
def redis_db_object_with_resources(redis_my, resource_foo) -> redis_adapter.RedisDB:
    import asyncio
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT)
    asyncio.ensure_future(test_adapter_obj.add_resource(resource_foo))
    asyncio.ensure_future(test_adapter_obj.set_qrm_status(status='active'))
    asyncio.ensure_future(test_adapter_obj.get_all_resources_dict())
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


@pytest.fixture(scope='function')
def post_to_http_server(loop, aiohttp_client):
    app = web.Application(loop=loop)
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock)
    app.router.add_post(qrm_http_server.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_http_server.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_http_server.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    yield loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def post_to_http_server_mock(loop, aiohttp_client):
    app = web.Application(loop=loop)
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock)
    app.router.add_post(qrm_http_server.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_http_server.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_http_server.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    yield loop.run_until_complete(aiohttp_client(app))

@pytest.fixture(scope='function')
def qrm_backend_with_db(redis_db_object) -> QueueManagerBackEnd:
    return QueueManagerBackEnd(redis_port=REDIS_PORT)
