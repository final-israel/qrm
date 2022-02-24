import json

from qrm_server import qrm_http_server
from qrm_client.qrm_http_client import QrmClient
from qrm_defs.resource_definition import ResourcesRequest, ResourcesByName, ACTIVE_STATUS, PENDING_STATUS


def test_qrm_http_client_get_root_url_debug(qrm_http_client_with_server_mock_debug_prints: QrmClient):
    resp = qrm_http_client_with_server_mock_debug_prints.get_root_url()
    assert resp.status_code == 200


def test_qrm_http_client_get_root_url(qrm_http_client_with_server_mock: QrmClient):
    resp = qrm_http_client_with_server_mock.get_root_url()
    assert resp.status_code == 200
    assert resp.text == '1'


def test_qrm_http_client__send_cancel(qrm_http_client_with_server_mock, default_test_token):
    resp = qrm_http_client_with_server_mock._send_cancel(token=default_test_token)
    assert resp.status_code == 200
    assert resp.text == qrm_http_server.canceled_token_msg(default_test_token)


def test_qrm_http_client_send_cancel(qrm_http_client_with_server_mock, default_test_token):
    resp = qrm_http_client_with_server_mock.send_cancel(token=default_test_token)
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
    assert result.get('token') == default_test_token


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


def test_qrm_http_client_get_is_server_up(qrm_http_client_with_server_mock, default_test_token):
    resp_data = qrm_http_client_with_server_mock.wait_for_server_up()
    assert isinstance(resp_data, dict)
    assert resp_data.get('status')


def test_qrm_http_client_wait_for_token_ready(qrm_http_client_with_server_mock_debug_prints, default_test_token):
    qrm_http_client_with_server_mock_debug_prints.token = default_test_token
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['res1'], count=1)
    rr.names.append(rbs)
    resp_data = qrm_http_client_with_server_mock_debug_prints.wait_for_token_ready(default_test_token,
                                                                                   polling_sleep_time=0.1)
    assert isinstance(resp_data, dict)
    assert resp_data.get('token') == default_test_token
    assert resp_data.get('request_complete')
    assert 'res1' in resp_data.get('names')


def test_qrm_http_client_send_cancel_get_bad_response_400(qrm_server_mock_for_client_with_error):
    qrm_client_obj = QrmClient(server_ip=qrm_server_mock_for_client_with_error.host,
                               server_port=qrm_server_mock_for_client_with_error.port,
                               user_name='test_user')
    resp = qrm_client_obj.send_cancel(token='12345')
    assert resp.status_code == 400


def test_mgmt_client_get_resource_status(mgmt_client, qrm_client):
    qrm_client.wait_for_server_up()

    # r1 starts with active status:
    r1_status = mgmt_client.get_resource_status('r1')
    assert r1_status == ACTIVE_STATUS


def test_mgmt_client_set_resource_status(mgmt_client, qrm_client):
    qrm_client.wait_for_server_up()
    r1_status = mgmt_client.get_resource_status('r1')
    assert r1_status == ACTIVE_STATUS
    mgmt_client.set_resource_status('r1', PENDING_STATUS)
    r1_status = mgmt_client.get_resource_status('r1')
    assert r1_status == PENDING_STATUS
