import sys
from pathlib import Path
here = Path(__file__).resolve().parent.parent
sys.path.append(f'{here}')
import asyncio
import logging
import pytest
from aiohttp import web
import qrm_server.qrm_http_server
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server import qrm_http_server
from qrm_server.resource_definition import Resource, ACTIVE_STATUS
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc, \
    ResourcesRequest, ResourcesRequestResponse
from pytest_httpserver import HTTPServer
from qrm_client.qrm_http_client import QrmClient, ManagementClient
from werkzeug.wrappers import Request, Response
REDIS_PORT = 6379
from multiprocessing import Process
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')
wait_for_test_call_times = 0
TEST_TOKEN = 'token1234'



# noinspection PyMethodMayBeStatic
class QueueManagerBackEndMock(QrmIfc):
    for_test_is_request_active: bool = False
    get_filled_request_obj: ResourcesRequestResponse

    async def cancel_request(self, token: str) -> None:
        print('#######  using cancel_request in QueueManagerBackEndMock ####### ')
        return

    async def new_request(self, resources_request: ResourcesRequest) -> ResourcesRequestResponse:
        resources_request_res = ResourcesRequestResponse()
        resources_request_res.token = resources_request.token
        return resources_request_res

    async def is_request_active(self, token: str) -> bool:
        return self.for_test_is_request_active

    async def get_new_token(self, token: str) -> str:
        return f'{token}_new'

    async def get_resource_req_resp(self, token: str) -> ResourcesRequestResponse:
        return self.get_filled_request_obj

    async def init_backend(self) -> None:
        pass


@pytest.fixture(scope='session')
def default_test_token() -> str:
    return TEST_TOKEN


@pytest.fixture(scope='function')
def qrm_server_mock_for_client(httpserver: HTTPServer, default_test_token: str) -> HTTPServer:
    rrr_obj = ResourcesRequestResponse()
    rrr_obj.token = default_test_token
    rrr_json = rrr_obj.as_json()
    httpserver.expect_request(f'{qrm_http_server.URL_GET_ROOT}').respond_with_data("1")
    httpserver.expect_request(
        f'{qrm_http_server.URL_POST_CANCEL_TOKEN}').respond_with_data(qrm_http_server.canceled_token_msg(TEST_TOKEN))
    httpserver.expect_request(qrm_http_server.URL_POST_NEW_REQUEST).respond_with_json(rrr_json)
    httpserver.expect_request(qrm_http_server.URL_GET_TOKEN_STATUS).respond_with_json(rrr_json)
    httpserver.expect_request(qrm_http_server.URL_GET_IS_SERVER_UP).respond_with_json({'status': True})
    return httpserver


@pytest.fixture(scope='function')
def qrm_server_mock_for_client_for_debug(httpserver: HTTPServer, default_test_token) -> HTTPServer:
    def handler(request: Request):
        print('#### start debug print ####')
        print(request)
        print('#### end debug print ####')
        res = Response()
        res.status_code = 200
        return res

    def handler_for_wait_for_test(request: Request):
        print('#########')
        global wait_for_test_call_times
        rrr_obj = ResourcesRequestResponse()
        rrr_obj.token = default_test_token
        if wait_for_test_call_times > 1:
            rrr_obj.request_complete = True
            rrr_obj.names.append('res1')
        rrr_json = rrr_obj.as_json()
        res = Response(rrr_json, status=200, content_type="application/json")
        wait_for_test_call_times += 1
        return res
    httpserver.expect_request(f'{qrm_http_server.URL_GET_ROOT}').respond_with_handler(handler)
    httpserver.expect_request(f'{qrm_http_server.URL_POST_CANCEL_TOKEN}').respond_with_handler(handler)
    httpserver.expect_request(qrm_http_server.URL_GET_TOKEN_STATUS).respond_with_handler(handler_for_wait_for_test)
    yield httpserver


@pytest.fixture(scope='function')
def qrm_server_mock_for_client_with_error(httpserver: HTTPServer) -> HTTPServer:
    httpserver.expect_request(f'{qrm_http_server.URL_POST_CANCEL_TOKEN}').respond_with_response(Response(status=400))
    return httpserver


@pytest.fixture(scope='function')
def qrm_http_client_with_server_mock(qrm_server_mock_for_client: HTTPServer) -> QrmClient:
    qrm_client_obj = QrmClient(server_ip=qrm_server_mock_for_client.host,
                               server_port=qrm_server_mock_for_client.port,
                               user_name='test_user')
    return qrm_client_obj


@pytest.fixture(scope='function')
def qrm_http_client_with_server_mock_debug_prints(qrm_server_mock_for_client_for_debug: HTTPServer) -> QrmClient:
    qrm_client_obj = QrmClient(server_ip=qrm_server_mock_for_client_for_debug.host,
                               server_port=qrm_server_mock_for_client_for_debug.port,
                               user_name='test_user')
    return qrm_client_obj


@pytest.fixture(scope='session')
def qrm_backend_mock() -> QueueManagerBackEndMock:
    return QueueManagerBackEndMock()


@pytest.fixture(scope='function')
def qrm_backend_mock_cls() -> QueueManagerBackEndMock:
    return QueueManagerBackEndMock()


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
    app.router.add_get(management_server.MGMT_STATUS_API, management_server.status)
    app.router.add_post(management_server.SET_SERVER_STATUS, management_server.set_server_status)
    app.router.add_post(management_server.SET_RESOURCE_STATUS, management_server.set_resource_status)
    app.router.add_post(management_server.ADD_JOB_TO_RESOURCE, management_server.add_job_to_resource)
    app.router.add_post(management_server.REMOVE_JOB, management_server.remove_job)
    yield loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def post_to_http_server(loop, aiohttp_client):
    app = web.Application(loop=loop)
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock())
    app.router.add_post(qrm_http_server.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_http_server.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_http_server.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    yield loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def post_to_http_server2(loop, aiohttp_server):
    app = web.Application(loop=loop)
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock())
    app.router.add_post(qrm_http_server.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_http_server.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_http_server.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    app.router.add_get(qrm_http_server.URL_GET_ROOT, qrm_http_server.root_url)
    yield loop.run_until_complete(aiohttp_server(app))


@pytest.fixture(scope='function')
def qrm_http_server_for_system(aiohttp_unused_port, redis_db_object) -> dict:
    port = aiohttp_unused_port()
    p = Process(target=qrm_server.qrm_http_server.run_server, args=(port,))
    p.start()
    yield {'http_port': port}
    p.kill()


@pytest.fixture(scope='function')
def qrm_http_server_for_system_pending(aiohttp_unused_port, redis_db_object) -> dict:
    pending = True
    port = aiohttp_unused_port()
    p = Process(target=qrm_server.qrm_http_server.run_server, args=(port, pending,))
    p.start()
    yield {'http_port': port}
    p.kill()


@pytest.fixture(scope='function')
def qrm_management_server(aiohttp_unused_port, redis_db_object) -> dict:
    port = aiohttp_unused_port()
    p = Process(target=qrm_server.management_server.main, kwargs={'port': port})
    p.start()
    yield {'management_port': port}
    p.kill()


@pytest.fixture(scope='function')
def full_qrm_servers_ports(aiohttp_unused_port, qrm_http_server_for_system,
                           qrm_management_server, redis_db_object) -> dict:
    ports_dict = {}

    r1 = asyncio.gather(redis_db_object.add_resource(Resource(name='r1', type='server', status=ACTIVE_STATUS)))
    r2 = asyncio.gather(redis_db_object.add_resource(Resource(name='r2', type='server', status=ACTIVE_STATUS)))
    r3 = asyncio.gather(redis_db_object.add_resource(Resource(name='r3', type='server', status=ACTIVE_STATUS)))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(r1)
    loop.run_until_complete(r2)
    loop.run_until_complete(r3)
    ports_dict.update(qrm_management_server)
    ports_dict.update(qrm_http_server_for_system)
    return ports_dict


@pytest.fixture(scope='function')
def qrm_client(full_qrm_servers_ports: dict) -> QrmClient:
    client = QrmClient(server_ip='127.0.0.1',
                       server_port=full_qrm_servers_ports['http_port'],
                       user_name='test_user')
    client.wait_for_server_up()
    return client


@pytest.fixture(scope='function')
def qrm_client_pending(full_qrm_servers_ports_pending_logic: dict) -> QrmClient:
    client = QrmClient(server_ip='127.0.0.1',
                       server_port=full_qrm_servers_ports_pending_logic['http_port'],
                       user_name='test_user')
    client.wait_for_server_up()
    return client


@pytest.fixture(scope='function')
def mgmt_client(full_qrm_servers_ports: dict) -> ManagementClient:
    client = ManagementClient(server_ip='127.0.0.1',
                              server_port=full_qrm_servers_ports['management_port'],
                              user_name='test_user')
    return client


@pytest.fixture(scope='function')
def mgmt_client_pending(full_qrm_servers_ports_pending_logic: dict) -> ManagementClient:
    client = ManagementClient(server_ip='127.0.0.1',
                              server_port=full_qrm_servers_ports_pending_logic['management_port'],
                              user_name='test_user')
    return client


@pytest.fixture(scope='function')
def full_qrm_servers_ports_pending_logic(aiohttp_unused_port, qrm_http_server_for_system_pending,
                                         qrm_management_server, redis_db_object) -> dict:
    ports_dict = {}

    r1 = asyncio.gather(redis_db_object.add_resource(Resource(name='r1', type='server', status=ACTIVE_STATUS)))
    r2 = asyncio.gather(redis_db_object.add_resource(Resource(name='r2', type='server', status=ACTIVE_STATUS)))
    r3 = asyncio.gather(redis_db_object.add_resource(Resource(name='r3', type='server', status=ACTIVE_STATUS)))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(r1)
    loop.run_until_complete(r2)
    loop.run_until_complete(r3)
    ports_dict.update(qrm_management_server)
    ports_dict.update(qrm_http_server_for_system_pending)
    return ports_dict


@pytest.fixture(scope='function')
def qrm_backend_with_db(redis_db_object) -> QueueManagerBackEnd:
    return QueueManagerBackEnd(redis_port=REDIS_PORT)
