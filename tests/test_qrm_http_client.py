import json
import pytest
from qrm_server import qrm_http_server
from qrm_server.resource_definition import Resource, ResourcesRequest, resource_request_response_from_json, \
    ResourcesRequestResponse
from qrm_client.qrm_http_client import QrmClient


def test_qrm_http_client_get_root_url(post_to_http_server_mock):
    server_ip = post_to_http_server_mock.host
    server_port = post_to_http_server_mock.port
    qrm_client_obj = QrmClient(server_ip=server_ip,
                               server_port=server_port,
                               user_name='test_user')

    resp = qrm_client_obj.get_root_url()
    resp_as_text = resp.text()
    assert resp.status == 200
    #assert resp_as_text == f'canceled token'


def test_qrm_http_client_send_cancel(post_to_http_server_mock):
    server_ip = post_to_http_server_mock.host
    server_port = post_to_http_server_mock .port
    qrm_client_obj = QrmClient(server_ip=server_ip,
                               server_port=server_port,
                               user_name='test_user')

    resp = qrm_client_obj.send_cancel()
    resp_as_text = resp.text()
    assert resp.status == 200
    assert resp_as_text == f'canceled token'



