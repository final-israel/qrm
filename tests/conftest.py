import asyncio
import logging
import pytest
import sys
import qrm_server.qrm_http_server
import qrm_defs.qrm_urls
import json
import os
from aiohttp import web
from pathlib import Path
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server import qrm_http_server
from qrm_defs.resource_definition import Resource, ACTIVE_STATUS
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc, \
    ResourcesRequest, ResourcesRequestResponse
from pytest_httpserver import HTTPServer
from qrm_client.qrm_http_client import QrmClient, ManagementClient
from werkzeug.wrappers import Request, Response
from multiprocessing import Process

TEST_TOKEN = 'token1234'
REDIS_PORT = 6379

here = Path(__file__).resolve().parent.parent
sys.path.append(f'{here}')
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
redis_my_proc = factories.redis_proc(port=REDIS_PORT)
redis_my = factories.redisdb('redis_my_proc')
wait_for_test_call_times = 0


def test_if_redis_server_is_up():
    stat = os.system('service redis-server status')
    if not stat:
        logging.error(
            f'"service redis-server status" return is = {stat}, test cant run with redis, run: "service redis-server stop"')
        try:
            stat = os.system('service redis-server stop')
        except Exception as e:
            logging.error(e)


test_if_redis_server_is_up()


def json_to_dict(json_str: str or dict) -> dict:
    if isinstance(json_str, str):
        return json.loads(json_str)
    else:
        return json_str


# noinspection PyMethodMayBeStatic
class QueueManagerBackEndMock(QrmIfc):
    for_test_is_request_active: bool = False
    get_filled_request_obj: ResourcesRequestResponse = ResourcesRequestResponse()

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

    async def stop_backend(self) -> None:
        pass


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def default_test_token() -> str:
    return TEST_TOKEN


@pytest.fixture(scope='function')
def qrm_server_mock_for_client(httpserver: HTTPServer, default_test_token: str) -> HTTPServer:
    # noinspection PyShadowingNames
    def new_request_handler(request: Request):
        req_json = request.json
        req_json = json_to_dict(req_json)
        rrr_obj = ResourcesRequestResponse(token=req_json['token'])
        rrr_json = rrr_obj.as_json()
        res = Response(rrr_json, status=200, content_type="application/json")
        return res

    rrr_obj = ResourcesRequestResponse()
    rrr_obj.token = default_test_token
    rrr_json = rrr_obj.as_json()
    httpserver.expect_request(f'{qrm_defs.qrm_urls.URL_GET_ROOT}').respond_with_data("1")
    httpserver.expect_request(
        f'{qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN}').respond_with_data(qrm_http_server.canceled_token_msg(TEST_TOKEN))
    httpserver.expect_request(qrm_defs.qrm_urls.URL_POST_NEW_REQUEST).respond_with_handler(new_request_handler)
    httpserver.expect_request(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS).respond_with_json(rrr_json)
    httpserver.expect_request(qrm_defs.qrm_urls.URL_GET_IS_SERVER_UP).respond_with_json({'status': True})
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

    httpserver.expect_request(f'{qrm_defs.qrm_urls.URL_GET_ROOT}').respond_with_handler(handler)
    httpserver.expect_request(f'{qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN}').respond_with_handler(handler)
    httpserver.expect_request(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS).respond_with_handler(handler_for_wait_for_test)
    yield httpserver


@pytest.fixture(scope='function')
def qrm_server_mock_for_client_with_error(httpserver: HTTPServer) -> HTTPServer:
    httpserver.expect_request(f'{qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN}').respond_with_response(Response(status=400))
    return httpserver


@pytest.fixture(scope='function')
def qrm_http_client_with_server_mock(qrm_server_mock_for_client: HTTPServer) -> QrmClient:
    qrm_client_obj = QrmClient(server_ip=qrm_server_mock_for_client.host,
                               server_port=qrm_server_mock_for_client.port,
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
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
async def redis_db_object(redis_my) -> redis_adapter.RedisDB:
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT, pubsub_polling_time=0.05)
    await test_adapter_obj.init_params_blocking()
    yield test_adapter_obj
    await test_adapter_obj.close()
    del test_adapter_obj


@pytest.fixture(scope='function')
async def redis_db_object_with_resources(redis_my, resource_foo) -> redis_adapter.RedisDB:
    test_adapter_obj = redis_adapter.RedisDB(redis_port=REDIS_PORT, pubsub_polling_time=0.05)
    await test_adapter_obj.init_params_blocking()
    await test_adapter_obj.add_resource(resource_foo)
    await test_adapter_obj.set_qrm_status(status='active')
    await test_adapter_obj.get_all_resources_dict()
    yield test_adapter_obj
    await test_adapter_obj.close()
    del test_adapter_obj


@pytest.fixture(scope='function')
def post_to_mgmt_server(event_loop, aiohttp_client):
    app = web.Application()
    management_server.init_redis()
    app.router.add_post(qrm_defs.qrm_urls.ADD_RESOURCES, management_server.add_resources)
    app.router.add_post(qrm_defs.qrm_urls.REMOVE_RESOURCES, management_server.remove_resources)
    app.router.add_get(qrm_defs.qrm_urls.MGMT_STATUS_API, management_server.status)
    app.router.add_post(qrm_defs.qrm_urls.SET_SERVER_STATUS, management_server.set_server_status)
    app.router.add_post(qrm_defs.qrm_urls.SET_RESOURCE_STATUS, management_server.set_resource_status)
    app.router.add_post(qrm_defs.qrm_urls.ADD_TAG_TO_RESOURCE, management_server.add_tag_to_resource)
    app.router.add_post(qrm_defs.qrm_urls.REMOVE_TAG_FROM_RESOURCE, management_server.remove_tag_from_resource)
    app.on_shutdown.append(management_server.close_redis)
    yield event_loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def post_to_http_server(event_loop, aiohttp_client):
    app = web.Application()
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock())
    app.router.add_post(qrm_defs.qrm_urls.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    yield event_loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def post_to_http_server2(event_loop, aiohttp_server):
    app = web.Application()
    qrm_http_server.init_qrm_back_end(QueueManagerBackEndMock())
    app.router.add_post(qrm_defs.qrm_urls.URL_POST_NEW_REQUEST, qrm_http_server.new_request)
    app.router.add_post(qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN, qrm_http_server.cancel_token)
    app.router.add_get(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS, qrm_http_server.get_token_status)
    app.router.add_get(qrm_defs.qrm_urls.URL_GET_ROOT, qrm_http_server.root_url)
    yield event_loop.run_until_complete(aiohttp_server(app))


@pytest.fixture(scope='function')
def qrm_http_server_for_system(unused_tcp_port_factory) -> dict:
    port = unused_tcp_port_factory()
    p = Process(target=qrm_server.qrm_http_server.run_server, args=(port,))
    p.start()
    yield {'http_port': port}
    p.terminate()


@pytest.fixture(scope='function')
def qrm_http_server_for_system_pending(unused_tcp_port_factory) -> dict:
    pending = True
    port = unused_tcp_port_factory()
    p = Process(target=qrm_server.qrm_http_server.run_server, args=(port, pending,))
    p.start()
    yield {'http_port': port}
    p.terminate()


@pytest.fixture(scope='function')
def qrm_management_server(unused_tcp_port_factory) -> dict:
    port = unused_tcp_port_factory()
    p = Process(target=qrm_server.management_server.main, kwargs={'listen_port': port})
    p.start()
    yield {'management_port': port}
    p.terminate()


@pytest.fixture(scope='function')
async def full_qrm_servers_ports(unused_tcp_port_factory, qrm_http_server_for_system,
                                 qrm_management_server, redis_db_object) -> dict:
    ports_dict = {}

    await redis_db_object.add_resource(Resource(name='r1', type='server', status=ACTIVE_STATUS))
    await redis_db_object.add_resource(Resource(name='r2', type='server', status=ACTIVE_STATUS))
    await redis_db_object.add_resource(Resource(name='r3', type='server', status=ACTIVE_STATUS))
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
def full_qrm_servers_ports_pending_logic(unused_tcp_port_factory, qrm_http_server_for_system_pending,
                                         qrm_management_server, redis_db_object) -> dict:
    ports_dict = {}

    r1 = asyncio.gather(redis_db_object.add_resource(Resource(name='r1', type='server',
                                                              status=ACTIVE_STATUS, tags=['server'])))
    r2 = asyncio.gather(redis_db_object.add_resource(Resource(name='r2', type='server',
                                                              status=ACTIVE_STATUS, tags=['server'])))
    r3 = asyncio.gather(redis_db_object.add_resource(Resource(name='r3', type='server',
                                                              status=ACTIVE_STATUS, tags=['server'])))
    r4 = asyncio.gather(redis_db_object.add_resource(Resource(name='v1', type='vlan',
                                                              status=ACTIVE_STATUS, tags=['vlan'])))
    r5 = asyncio.gather(redis_db_object.add_resource(Resource(name='v2', type='vlan',
                                                              status=ACTIVE_STATUS, tags=['vlan'])))
    r6 = asyncio.gather(redis_db_object.add_resource(Resource(name='v3', type='vlan',
                                                              status=ACTIVE_STATUS, tags=['vlan'])))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(r1)
    loop.run_until_complete(r2)
    loop.run_until_complete(r3)
    loop.run_until_complete(r4)
    loop.run_until_complete(r5)
    loop.run_until_complete(r6)
    ports_dict.update(qrm_management_server)
    ports_dict.update(qrm_http_server_for_system_pending)
    return ports_dict


@pytest.fixture(scope='function')
async def qrm_backend_with_db(redis_my) -> QueueManagerBackEnd:
    qrm_be = QueueManagerBackEnd(redis_port=REDIS_PORT)
    yield qrm_be
    await qrm_be.stop_backend()
