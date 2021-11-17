import json
import time

import pytest

import redis_adapter
from qrm_server import management_server


async def test_add_resource(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.ADD_RESOURCES, data=json.dumps(['resource_1']))
    assert resp.status == 200


async def test_resource_added_to_db(post_to_mgmt_server, redis_db_object):
    await post_to_mgmt_server.post(management_server.ADD_RESOURCES, data=json.dumps(['resource_2']))
    all_resources = await redis_db_object.get_all_resources()
    assert len(all_resources) == 1


async def test_remove_resource(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_3')
    resp = await post_to_mgmt_server.post(management_server.REMOVE_RESOURCES, data=json.dumps(['resource_2']))
    assert resp.status == 200


async def test_remove_resource_from_db(post_to_mgmt_server, redis_db_object):
    await redis_db_object.get_qrm_status()
    await redis_db_object.add_resource(resource_name='resource_2')
    await redis_db_object.get_qrm_status()
    await redis_db_object.add_resource(resource_name='resource_3')
    resp = await post_to_mgmt_server.post(management_server.REMOVE_RESOURCES, data=json.dumps(['resource_2']))
    all_resources = await redis_db_object.get_all_resources()
    assert len(all_resources) == 1


async def test_remove_non_existing_resource_from_db(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_3')
    resp = await post_to_mgmt_server.post(management_server.REMOVE_RESOURCES, data=json.dumps(['resource_2']))
    all_resources = await redis_db_object.get_all_resources()
    assert resp.status == 200
    assert len(all_resources) == 1


async def test_basic_status_empty_db(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('resources_status') == {}


async def test_status_one_resource_with_status(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_1')
    await redis_db_object.set_resource_status(resource_name='resource_1', status='active')
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'


async def test_status_multiple_resources_with_status(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_1')
    await redis_db_object.set_resource_status(resource_name='resource_1', status='active')
    await redis_db_object.add_resource(resource_name='resource_2')
    await redis_db_object.set_resource_status(resource_name='resource_2', status='disabled')
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'
    assert resp_as_dict['resources_status']['resource_2']['status'] == 'disabled'


async def test_status_resource_with_job(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_1')
    await redis_db_object.set_resource_status(resource_name='resource_1', status='active')
    await redis_db_object.add_job_to_resource('resource_1', {'id': 1, 'user': 'foo'})
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'
    assert resp_as_dict['resources_status']['resource_1']['jobs'] == [{'id': 1, 'user': 'foo'}, {}]


async def test_status_qrm_server(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'active'
    await redis_db_object.set_qrm_status('disabled')
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'disabled'


async def test_set_server_status(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps({'status': 'disabled'}))
    assert resp.status == 200
    assert await resp.text() == 'mew server status is: disabled\n'


async def test_set_server_status_not_allowed_status(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps({'status': 'foo'}))
    assert resp.status == 400
    assert await resp.text() == 'requested status is not allowed: foo\n'


async def test_set_server_status_missing_key_status(post_to_mgmt_server, redis_db_object):
    requested_dict = {'not_status': 'foo'}
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps(requested_dict))
    assert resp.status == 400
    assert await resp.text() == f'must specify the status in your request: {requested_dict}\n'


async def test_set_server_status_and_validate_status_output(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps({'status': 'disabled'}))
    assert resp.status == 200
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'disabled'


@pytest.mark.parametrize('status', ['active', 'disabled'])
async def test_set_resource_status(post_to_mgmt_server, redis_db_object, status):
    await redis_db_object.add_resource(resource_name='resource_1')
    qrm_status = await post_to_mgmt_server.get(management_server.STATUS)
    qrm_status_dict = await qrm_status.json()
    assert qrm_status_dict['resources_status']['resource_1']['status'] is None
    resp = await post_to_mgmt_server.post(management_server.SET_RESOURCE_STATUS,
                                          data=json.dumps({'status': status, 'resource_name': 'resource_1'}))
    assert resp.status == 200
    qrm_status = await post_to_mgmt_server.get(management_server.STATUS)
    qrm_status_dict = await qrm_status.json()
    assert qrm_status_dict['resources_status']['resource_1']['status'] == status


async def test_set_resource_status_resource_not_exist(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_RESOURCE_STATUS,
                                          data=json.dumps({'status': 'active', 'resource_name': 'resource_1'}))
    assert resp.status == 400
    assert await resp.text() == 'Error: resource resource_1 does not exist or status is not allowed\n'


async def test_set_resource_status_missing_key(post_to_mgmt_server, redis_db_object):
    req_dict = {'resource_name': 'resource_1'}
    resp = await post_to_mgmt_server.post(management_server.SET_RESOURCE_STATUS,
                                          data=json.dumps(req_dict))
    assert resp.status == 400
    assert await resp.text() == f'Error: must specify both status and resource_name in your request: {req_dict}\n'


async def test_add_job_to_resource(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_1')
    req_dict = {'resource_name': 'resource_1', 'job': {'job_id': 1, 'job_name': 'foo'}}
    resp = await post_to_mgmt_server.post(management_server.ADD_JOB_TO_RESOURCE,
                                          data=json.dumps(req_dict))
    qrm_status = await post_to_mgmt_server.get(management_server.STATUS)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [req_dict['job'], {}]


async def test_remove_job_from_resource(post_to_mgmt_server, redis_db_object):
    await redis_db_object.add_resource(resource_name='resource_1')
    req_dict = {'resource_name': 'resource_1', 'job': {'id': 1, 'job_name': 'foo'}}
    resp = await post_to_mgmt_server.post(management_server.ADD_JOB_TO_RESOURCE,
                                          data=json.dumps(req_dict))
    assert resp.status == 200
    qrm_status = await post_to_mgmt_server.get(management_server.STATUS)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [req_dict['job'], {}]
    await post_to_mgmt_server.post(management_server.REMOVE_JOB,
                                   data=json.dumps({'id': 1, 'resources': ['resource_1']}))
    qrm_status = await post_to_mgmt_server.get(management_server.STATUS)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [{}]


async def test_add_existing_resource():
    assert False
