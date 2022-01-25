import copy
import json
import pytest
from qrm_server import qrm_http_server


async def test_send_request(post_to_http_server, resource_dict_1):
    resp = await post_to_http_server.post(qrm_http_server.URL_POST_NEW_REQUEST, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
