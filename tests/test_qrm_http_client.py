import json

from qrm_server import qrm_http_server
from qrm_client.qrm_http_client import QrmClient
from qrm_server.resource_definition import ResourcesRequest, ResourcesRequestResponse


def test_qrm_http_client_get_root_url_debug(qrm_http_client_with_server_mock_debug_prints: QrmClient):
    resp = qrm_http_client_with_server_mock_debug_prints.get_root_url()
    assert resp.status_code == 200


def test_qrm_http_client_get_root_url(qrm_http_client_with_server_mock: QrmClient):
    resp = qrm_http_client_with_server_mock.get_root_url()
    assert resp.status_code == 200
    assert resp.text == '1'


def test_qrm_http_client__send_cancel(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    resp = qrm_http_client_with_server_mock._send_cancel()
    assert resp.status_code == 200
    assert resp.text == qrm_http_server.canceled_token_msg(default_test_token)


def test_qrm_http_client_send_cancel(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    resp = qrm_http_client_with_server_mock.send_cancel()
    assert resp


def test_qrm_http_client__new_request(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp = qrm_http_client_with_server_mock._new_request(data_json=rr.as_json())
    resp_json = resp.json()
    resp_data = json.loads(resp_json)
    assert resp.status_code == 200
    assert resp_data.get('token') == default_test_token


def test_qrm_http_client_new_request(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    rr = ResourcesRequest()
    rr.token = default_test_token
    result = qrm_http_client_with_server_mock.new_request(data_json=rr.as_json())
    assert result == default_test_token


def test_qrm_http_client__get_token_status(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    resp = qrm_http_client_with_server_mock._get_token_status(default_test_token)
    resp_json = resp.json()
    resp_data = json.loads(resp_json)
    assert resp.status_code == 200
    assert resp_data.get('token') == default_test_token
    assert resp_data.get('request_complete') is not None
    assert resp_data.get('names') is not None


def test_qrm_http_client_get_token_status(qrm_http_client_with_server_mock, default_test_token):
    qrm_http_client_with_server_mock.token = default_test_token
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp_data = qrm_http_client_with_server_mock.get_token_status(default_test_token)
    assert isinstance(resp_data, dict)
    assert resp_data.get('token') == default_test_token
    assert resp_data.get('request_complete') is not None
    assert resp_data.get('names') is not None


def test_qrm_http_client_send_cancel_get_bad_response_400(qrm_server_mock_for_client_with_error, default_test_token):
    qrm_client_obj = QrmClient(server_ip=qrm_server_mock_for_client_with_error.host,
                               server_port=qrm_server_mock_for_client_with_error.port,
                               user_name='test_user')
    resp = qrm_client_obj.send_cancel()
    assert resp is False





