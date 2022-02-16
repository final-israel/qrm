import json
import logging
import time

import pytest

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
    assert default_test_token in resp.get('token')
    assert resp.get('is_valid')


def test_client_new_requested_resource_does_not_exist(full_qrm_servers_ports, default_test_token):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    # no_resource does not exist in DB:
    rbn = ResourcesByName(names=['no_resource'], count=1)
    rr.names.append(rbn)
    resp = qrm_client_obj.new_request(rr.as_json())
    new_token = resp['token']
    qrm_client_obj.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client_obj.get_token_status(new_token)

    assert default_test_token in resp.get('token')
    assert resp.get('message')
    assert 'no resources named no_resource' == resp.get('message') or resp.get('message') != ''
    assert resp.get('is_valid') is False


def test_http_server_and_client_new_request_token_not_valid_and_no_servers(full_qrm_servers_ports, default_test_token):
    # request has only token and no resources, token is not valid.
    # server should return relevant meesage and is_valid = False
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp = qrm_client_obj.new_request(rr.as_json())
    new_token = resp['token']
    qrm_client_obj.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client_obj.get_token_status(new_token)
    assert 'contains names ' in resp.get('message')
    assert resp.get('is_valid') is False


def test_http_server_and_client_status_done(full_qrm_servers_ports, default_test_token):
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
    resp_2 = qrm_client_obj.get_token_status(resp.get('token'))
    n_try = 0
    while not resp_2.get('request_complete'):
        time.sleep(0.1)
        n_try += 1
        resp_2 = qrm_client_obj.get_token_status(resp.get('token'))
        print(f"debug ###### {n_try}")
        print(resp_2)
        if n_try >= 5:
            break
    qrm_client_obj.wait_for_token_ready(resp.get('token'), timeout=10)
    assert default_test_token in resp.get('token')
    assert resp_2.get('request_complete')
    assert 'r1' in resp_2.names