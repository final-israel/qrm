import json

import qrm_defs.qrm_urls
from qrm_server import qrm_http_server
from qrm_defs.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse


async def test_http_server_cancel_token(post_to_http_server):
    token = 'token1'
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    resp = await post_to_http_server.post(qrm_defs.qrm_urls.URL_POST_CANCEL_TOKEN,
                                          data=json.dumps(user_request.as_json()))

    resp_json = await resp.json()
    resp_dict = json.loads(resp_json)
    assert resp.status == 200
    assert resp_dict.get('message') == f'canceled token {token}'


# noinspection DuplicatedCode
async def test_http_server_new_request_new_token(post_to_http_server):
    token = 'token1'
    res_1 = Resource(name='res1', type='type1')
    res_2 = Resource(name='res2', type='type1')
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    print(post_to_http_server.host)
    print(post_to_http_server.port)
    resp = await post_to_http_server.post(qrm_defs.qrm_urls.URL_POST_NEW_REQUEST,
                                          data=json.dumps(user_request.as_json()))
    resp_json = await resp.json()
    resp_dict = json.loads(resp_json)
    expected_token = f'{token}_new'
    assert resp.status == 200
    assert resp_dict.get('token') == expected_token


# noinspection DuplicatedCode,PyTypeChecker
async def test_http_server_get_token_status_is_active(post_to_http_server, qrm_backend_mock_cls):
    token = 'token1'
    res_1 = Resource(name='res1', type='type1')
    res_2 = Resource(name='res2', type='type1')
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    queue_manager_back_end_mock = qrm_backend_mock_cls
    queue_manager_back_end_mock.for_test_is_request_active = True
    qrm_http_server.init_qrm_back_end(queue_manager_back_end_mock)
    resp = await post_to_http_server.get(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS, params={'token': token})
    resp_json = await resp.json()
    rrr_obj = ResourcesRequestResponse.from_json(resp_json)
    assert resp.status == 200
    assert rrr_obj.request_complete is False


# noinspection DuplicatedCode
async def test_http_server_get_token_status_is_done(post_to_http_server, qrm_backend_mock_cls):
    # setup start
    token = 'token1'
    res_1 = Resource(name='res1', type='type1')
    res_2 = Resource(name='res2', type='type1')
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    queue_manager_back_end_mock = qrm_backend_mock_cls
    queue_manager_back_end_mock.for_test_is_request_active = False
    expected_rrr_obj = ResourcesRequestResponse()
    expected_rrr_obj.token = token
    expected_rrr_obj.names = [res_1.name, res_2.name]
    queue_manager_back_end_mock.get_filled_request_obj = expected_rrr_obj
    qrm_http_server.init_qrm_back_end(queue_manager_back_end_mock)
    # setup end
    # test start
    resp = await post_to_http_server.get(qrm_defs.qrm_urls.URL_GET_TOKEN_STATUS, params={'token': token})
    resp_json = await resp.json()
    rrr_obj = ResourcesRequestResponse.from_json(resp_json)
    assert resp.status == 200
    assert rrr_obj.request_complete
    assert rrr_obj.token == token
    assert 'res1' in rrr_obj.names
    assert 'res2' in rrr_obj.names
