from qrm_server.resource_definition import Resource, ResourcesRequestResponse, ResourcesRequest, ResourcesByName
from qrm_client.qrm_http_client import QrmClient


# noinspection DuplicatedCode
def test_http_server_and_client_get_root_url(full_qrm_servers_ports, redis_db_object):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    out = qrm_client_obj.get_root_url()
    assert 'server up 1' == out.text


def test_client_new_request(full_qrm_servers_ports, default_test_token):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbs)
    resp = qrm_client_obj.new_request(rr.as_json())
    assert default_test_token in resp


def test_client_new_request_server_does_not_exist(full_qrm_servers_ports, default_test_token):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['no_server'], count=1)
    rr.names.append(rbs)
    resp = qrm_client_obj.new_request(rr.as_json())
    assert default_test_token in resp


def test_http_server_and_client_new_request_token_not_valid_and_no_servers(full_qrm_servers_ports, default_test_token):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp = qrm_client_obj.new_request(rr.as_json())
    assert default_test_token in resp


def test_http_server_and_client_new_wait(full_qrm_servers_ports, default_test_token):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp = qrm_client_obj.new_request(rr.as_json())
    assert default_test_token in resp