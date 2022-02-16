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
    new_token = resp.get('token')
    qrm_client_obj.wait_for_token_ready(new_token, timeout=2)
    resp_2 = qrm_client_obj.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp_2.get('request_complete')
    assert 'r1' in resp_2.get('names')


def test_full_request_two_resources(full_qrm_servers_ports, default_test_token):
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=full_qrm_servers_ports['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1', 'r2'], count=2)
    rr.names.append(rbn)
    resp = qrm_client_obj.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client_obj.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client_obj.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp.get('request_complete')
    assert 'r1' and 'r2' in resp.get('names')


def test_full_request_one_from_two_resources(full_qrm_servers_ports, default_test_token):
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=full_qrm_servers_ports['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    # count is 1:
    rbn = ResourcesByName(names=['r1', 'r2'], count=1)
    rr.names.append(rbn)
    resp = qrm_client_obj.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client_obj.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client_obj.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp.get('request_complete')
    assert 'r1' or 'r2' in resp.get('names')
    assert len(resp.get('names')) == 1


def test_full_request_job_blocking_in_queue_release_by_cancel(full_qrm_servers_ports, default_test_token):
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=full_qrm_servers_ports['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_obj.new_request(rr.as_json())
    token_1 = resp.get('token')
    qrm_client_obj.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_obj.get_token_status(token_1)
    assert 'r1' in resp.get('names')

    # now send new request on the same resource, shouldn't get filled:
    rr = ResourcesRequest()
    rr.token = 'req_2_token'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    resp_token_2 = qrm_client_obj.new_request(rr.as_json())
    token_2 = resp_token_2.get('token')
    qrm_client_obj.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp_token_2 = qrm_client_obj.get_token_status(token_2)
    assert not resp_token_2.get('request_complete')
    assert resp_token_2.get('names') == []

    # send cancel on token1 will release the token_2 req and it should be filled
    qrm_client_obj.send_cancel(token_1)
    qrm_client_obj.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp_token_2 = qrm_client_obj.get_token_status(token_2)
    assert resp_token_2.get('request_complete')
    assert resp_token_2.get('names') == ['r1']


def test_full_req_cancel_after_partial_fill(full_qrm_servers_ports, default_test_token):
    # cancel the token after partially fill request and verify resource released
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=full_qrm_servers_ports['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_obj.new_request(rr.as_json())
    token_1 = resp.get('token')
    qrm_client_obj.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_obj.get_token_status(token_1)
    assert 'r1' in resp.get('names')

    # now send request on both r1 and r2, req should be active since it only partially filled
    rr = ResourcesRequest()
    rr.token = 'req_2_token'
    rbn = ResourcesByName(names=['r1', 'r2'], count=2)
    rr.names.append(rbn)
    resp_token_2 = qrm_client_obj.new_request(rr.as_json())
    token_2 = resp_token_2.get('token')
    qrm_client_obj.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    # token is not ready:
    resp_token_2 = qrm_client_obj.get_token_status(token_2)
    assert not resp_token_2.get('request_complete')

    # req_3 contains r2 as well:
    rr = ResourcesRequest()
    rr.token = 'req_3_token'
    rbn = ResourcesByName(names=['r2'], count=1)
    rr.names.append(rbn)
    resp_token_3 = qrm_client_obj.new_request(rr.as_json())
    token_3 = resp_token_3.get('token')
    qrm_client_obj.wait_for_token_ready(token_3, timeout=0.2, polling_sleep_time=0.1)

    # req_3 not completed since token_2 is blocking:
    assert not resp_token_3.get('request_complete')

    # after cancel token_2, token_3 should be filled:
    qrm_client_obj.send_cancel(token_2)
    qrm_client_obj.wait_for_token_ready(token_3, timeout=0.2, polling_sleep_time=0.1)
    resp_token_3 = qrm_client_obj.get_token_status(token_3)
    assert resp_token_3.get('request_complete')
    assert resp_token_3.get('names') == ['r2']
