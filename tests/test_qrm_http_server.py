import copy
import json
import pytest
from qrm_server import qrm_http_server
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse


async def test_http_server_cancel_token(post_to_http_server):
    token = 'token1'
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    resp = await post_to_http_server.post(qrm_http_server.URL_POST_CANCEL_TOKEN,
                                          data=json.dumps(user_request.as_json()))

    resp_as_text = await resp.text()
    assert resp.status == 200
    assert resp_as_text == f'canceled token {token}'


async def test_http_server_new_request_same_token(post_to_http_server):
    token = 'token1'
    res_1 = Resource(name='res1', type='type1')
    res_2 = Resource(name='res2', type='type1')
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    resp = await post_to_http_server.post(qrm_http_server.URL_POST_NEW_REQUEST,
                                          data=json.dumps(user_request.as_json()))
    resp_json = await resp.json()
    resp_dict = json.loads(resp_json)
    expected_token = f'{token}_new'
    assert resp.status == 200
    assert resp_dict.get('token') == expected_token

