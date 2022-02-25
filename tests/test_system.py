import pytest
import time

from qrm_defs.resource_definition import ResourcesRequest, ResourcesByName, PENDING_STATUS, ACTIVE_STATUS


def test_http_server_and_client_get_root_url(qrm_client, redis_db_object):
    out = qrm_client.get_root_url()
    assert 'server up 1' in out.text


def test_client_new_request(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbs)
    resp = qrm_client.new_request(rr.as_json())
    assert default_test_token in resp.get('token')
    assert resp.get('is_valid')


def test_client_new_requested_resource_does_not_exist(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    # no_resource does not exist in DB:
    rbn = ResourcesByName(names=['no_resource'], count=1)
    rr.names.append(rbn)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp['token']
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client.get_token_status(new_token)

    assert default_test_token in resp.get('token')
    assert resp.get('message')
    assert 'no resources named no_resource' == resp.get('message') or resp.get('message') != ''
    assert resp.get('is_valid') is False


def test_http_server_and_client_new_request_token_not_valid_and_no_servers(qrm_client, default_test_token):
    # request has only token and no resources, token is not valid.
    # server should return relevant meesage and is_valid = False
    rr = ResourcesRequest()
    rr.token = default_test_token
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp['token']
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client.get_token_status(new_token)
    assert 'contains names ' in resp.get('message')
    assert resp.get('is_valid') is False


def test_http_server_and_client_status_done(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbs)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp_2 = qrm_client.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp_2.get('request_complete')
    assert 'r1' in resp_2.get('names')


def test_full_request_two_resources(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1', 'r2'], count=2)
    rr.names.append(rbn)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp.get('request_complete')
    assert 'r1' and 'r2' in resp.get('names')


def test_full_request_job_blocking_in_queue_release_by_cancel(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client.new_request(rr.as_json())
    token_1 = resp.get('token')
    qrm_client.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client.get_token_status(token_1)
    assert 'r1' in resp.get('names')

    # now send new request on the same resource, shouldn't get filled:
    rr = ResourcesRequest()
    rr.token = 'req_2_token'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    resp_token_2 = qrm_client.new_request(rr.as_json())
    token_2 = resp_token_2.get('token')
    time.sleep(0.2)  # just sleep to allow the server to handle the request (it shouldn't get filled)
    resp_token_2 = qrm_client.get_token_status(token_2)
    assert not resp_token_2.get('request_complete')
    assert resp_token_2.get('names') == []

    # send cancel on token1 will release the token_2 req and it should be filled
    qrm_client.send_cancel(token_1)
    qrm_client.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp_token_2 = qrm_client.get_token_status(token_2)
    assert resp_token_2.get('request_complete')
    assert resp_token_2.get('names') == ['r1']


def test_full_req_cancel_after_partial_fill(qrm_client, default_test_token):
    # cancel the token after partially fill request and verify resource released
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client.new_request(rr.as_json())
    token_1 = resp.get('token')
    qrm_client.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client.get_token_status(token_1)
    assert 'r1' in resp.get('names')

    # now send request on both r1 and r2, req should be active since it only partially filled
    rr = ResourcesRequest()
    rr.token = 'req_2_token'
    rbn = ResourcesByName(names=['r1', 'r2'], count=2)
    rr.names.append(rbn)
    resp_token_2 = qrm_client.new_request(rr.as_json())
    token_2 = resp_token_2.get('token')
    time.sleep(0.2)  # just sleep to allow the server to handle the request (it shouldn't get filled)
    # token is not ready:
    resp_token_2 = qrm_client.get_token_status(token_2)
    assert not resp_token_2.get('request_complete')

    # req_3 contains r2 as well:
    rr = ResourcesRequest()
    rr.token = 'req_3_token'
    rbn = ResourcesByName(names=['r2'], count=1)
    rr.names.append(rbn)
    resp_token_3 = qrm_client.new_request(rr.as_json())
    token_3 = resp_token_3.get('token')
    time.sleep(0.2)  # just sleep to allow the server to handle the request (it shouldn't get filled)

    # req_3 not completed since token_2 is blocking:
    assert not resp_token_3.get('request_complete')

    # after cancel token_2, token_3 should be filled:
    qrm_client.send_cancel(token_2)
    qrm_client.wait_for_token_ready(token_3, timeout=0.2, polling_sleep_time=0.1)
    resp_token_3 = qrm_client.get_token_status(token_3)
    assert resp_token_3.get('request_complete')
    assert resp_token_3.get('names') == ['r2']


def test_http_server_and_client_status_done_for_resources_r1_and_r2_resources(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['r1', 'r2'], count=2)
    rr.names.append(rbs)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp_2 = qrm_client.get_token_status(new_token)
    assert resp_2.get('request_complete')
    assert 'r1' in resp_2.get('names')
    assert 'r2' in resp_2.get('names')


def test_full_request_one_from_two_resources(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    # count is 1:
    rbn = ResourcesByName(names=['r1', 'r2'], count=1)
    rr.names.append(rbn)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client.wait_for_token_ready(new_token, timeout=2)
    resp = qrm_client.get_token_status(new_token)
    assert default_test_token in new_token
    assert resp.get('request_complete')
    assert 'r1' or 'r2' in resp.get('names')
    assert len(resp.get('names')) == 1


def test_http_server_and_client_cancel(qrm_client, default_test_token):
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbs = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbs)
    resp = qrm_client.new_request(rr.as_json())
    new_token = resp.get('token')
    qrm_client.wait_for_token_ready(new_token, timeout=120)
    resp_2 = qrm_client.get_token_status(new_token)
    resp = qrm_client.send_cancel(new_token)
    assert default_test_token in new_token
    assert resp_2.get('request_complete')
    assert 'r1' in resp_2.get('names')


def test_resource_block_on_pending(qrm_client_pending, default_test_token, mgmt_client_pending):
    # resource active
    # send new request -> resource move to pending
    # send cancel -> resource is still pending
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_pending.new_request(rr.as_json())
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS
    token_1 = resp.get('token')
    time.sleep(0.2)
    resp = qrm_client_pending.get_token_status(token_1)
    assert not resp.get('request_complete')

    # send cancel -> resource move to pending:
    qrm_client_pending.send_cancel(token_1)
    resp = qrm_client_pending.get_token_status(token_1)
    assert resp.get('request_complete')
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS


def test_new_move_pending_change_to_active_cancel_move_to_pending(qrm_client_pending, default_test_token,
                                                                  mgmt_client_pending):
    # resource active
    # send new request -> resource move to pending
    # move resource to active -> token_1 filled
    # send token_2 -> waiting in queue
    # cancel token_1 -> resource move to pending, token_2 still waiting in queue
    # move resource to active -> token_2 filled
    rr = ResourcesRequest()
    rr.token = default_test_token
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_pending.new_request(rr.as_json())
    token_1 = resp.get('token')
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS

    # move r1 to active, token_1 filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    assert mgmt_client_pending.get_resource_status('r1') == ACTIVE_STATUS
    qrm_client_pending.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_pending.get_token_status(token_1)
    assert resp.get('request_complete')
    assert ['r1'] == resp.get('names')

    # send token_2 -> waiting in queue:
    rr = ResourcesRequest()
    rr.token = 'token_2'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    resp_2 = qrm_client_pending.new_request(rr.as_json())
    token_2 = resp_2.get('token')

    # cancel token_1 -> resource move to pending, token_2 still waiting in queue
    qrm_client_pending.send_cancel(token_1)
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS
    resp_2 = qrm_client_pending.get_token_status(token_2)
    assert not resp_2.get('request_complete')

    # move resource to active -> token_2 filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    qrm_client_pending.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp_2 = qrm_client_pending.get_token_status(token_2)
    assert resp_2.get('request_complete')
    assert ['r1'] == resp_2.get('names')


def test_resource_block_on_pending_job_wait(qrm_client_pending, mgmt_client_pending):
    # send new request -> fill
    # send token_2 -> verify waiting in queue
    # send cancel -> resource move to pending
    # move resource to active -> job2 filled
    rr = ResourcesRequest()
    rr.token = 'token_1'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_pending.new_request(rr.as_json())
    token_1 = resp.get('token')
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS

    # move r1 to active, request filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    assert mgmt_client_pending.get_resource_status('r1') == ACTIVE_STATUS
    qrm_client_pending.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_pending.get_token_status(token_1)
    assert resp.get('request_complete')
    assert ['r1'] == resp.get('names')

    # send job2 -> verify waiting in queue:
    rr = ResourcesRequest()
    rr.token = 'token_2'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    resp2 = qrm_client_pending.new_request(rr.as_json())
    token_2 = resp2.get('token')
    assert mgmt_client_pending.get_resource_status('r1') == ACTIVE_STATUS
    time.sleep(0.1)  # just to allow the server handle the request
    resp2 = qrm_client_pending.get_token_status(token_2)
    assert not resp2.get('request_complete')

    # send cancel on token_1 -> resource move to pending
    qrm_client_pending.send_cancel(token_1)
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS

    # move resource to active -> job2 filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    qrm_client_pending.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp2 = qrm_client_pending.get_token_status(token_2)
    assert resp.get('names') == ['r1']


@pytest.mark.skip
def test_resource_block_on_res_pending_job_with_one_pending_job(qrm_client_pending, default_test_token,
                                                                mgmt_client_pending):
    # send new request -> fill
    # send job2 -> verify waiting in queue
    # send cancel -> resource move to pending
    # move resource to active -> job2 filled

    raise NotImplementedError


def test_new_token_accepted_not_move_to_pending(qrm_client_pending, mgmt_client_pending):
    # send new request -> active
    # move resource to active -> request filled
    # cancel the new token -> resource is active
    # resend the new token -> accepted and resource active

    rr = ResourcesRequest()
    rr.token = 'token_1'
    rbn = ResourcesByName(names=['r1'], count=1)
    rr.names.append(rbn)
    # r1 is now with active job:
    resp = qrm_client_pending.new_request(rr.as_json())
    token_1 = resp.get('token')
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS

    # move r1 to active, request filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    assert mgmt_client_pending.get_resource_status('r1') == ACTIVE_STATUS
    qrm_client_pending.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_pending.get_token_status(token_1)
    assert resp.get('request_complete')
    assert ['r1'] == resp.get('names')

    # cancel the new token -> resource is active:
    qrm_client_pending.send_cancel(token_1)
    assert mgmt_client_pending.get_resource_status('r1') == ACTIVE_STATUS

    # resend the new token -> accepted and resource active:
    rr = ResourcesRequest()
    rr.token = token_1
    resp = qrm_client_pending.new_request(rr.as_json())
    new_token = resp.get('token')
    assert new_token == token_1
    resp = qrm_client_pending.get_token_status(new_token)
    assert resp['names'] == ['r1']


def test_pending_request_one_res_from_two_another_same_req(qrm_client_pending, mgmt_client_pending):
    # send new request 1 res from 2 res -> 1 res is pending
    # send same request -> the second res is pending
    # move res_1 to active -> req_1 filled
    # move res_2 to active -> req_2 filled

    rr = ResourcesRequest()
    rr.token = 'token_1'
    rbn = ResourcesByName(names=['r1', 'r2'], count=1)
    rr.names.append(rbn)
    # send new request 1 res from 2 res -> 1 res is pending:
    resp = qrm_client_pending.new_request(rr.as_json())
    token_1 = resp.get('token')
    # only one resource is in pending state:
    res_statuses = [mgmt_client_pending.get_resource_status('r1'), mgmt_client_pending.get_resource_status('r2')]
    assert PENDING_STATUS in res_statuses
    assert ACTIVE_STATUS in res_statuses

    # send same request -> the second res is pending:
    rr = ResourcesRequest()
    rr.token = 'token_2'
    rbn = ResourcesByName(names=['r1', 'r2'], count=1)
    rr.names.append(rbn)
    # send new request 1 res from 2 res -> 1 res is pending:
    resp = qrm_client_pending.new_request(rr.as_json())
    token_2 = resp.get('token')
    # both resources are pending:
    assert mgmt_client_pending.get_resource_status('r1') == PENDING_STATUS
    assert mgmt_client_pending.get_resource_status('r2') == PENDING_STATUS

    # move res_1 to active -> req_1 filled:
    mgmt_client_pending.set_resource_status('r1', ACTIVE_STATUS)
    qrm_client_pending.wait_for_token_ready(token_1, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_pending.get_token_status(token_1)
    assert resp.get('request_complete')
    assert len(resp.get('names')) == 1
    assert 'r1' or 'r2' in resp.get('names')

    # move res_2 to active -> req_2 filled:
    mgmt_client_pending.set_resource_status('r2', ACTIVE_STATUS)
    qrm_client_pending.wait_for_token_ready(token_2, timeout=0.2, polling_sleep_time=0.1)
    resp = qrm_client_pending.get_token_status(token_2)
    assert resp.get('request_complete')
    assert len(resp.get('names')) == 1
    assert 'r1' or 'r2' in resp.get('names')


@pytest.mark.skip
async def test_basic_recovery(mgmt_client_pending, redis_db_object):
    # send new request -> pending
    # shut down server
    # start new server -> request still pending
    # move resource to active -> request filled

    raise NotImplementedError


def test_token_status_unknown_token(qrm_client):
    qrm_client.wait_for_server_up()
    resp = qrm_client.get_token_status('unknown_token')
    assert 'unknown token in qrm' in resp.get('message')
    assert not resp.get('is_valid')
