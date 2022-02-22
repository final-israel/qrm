import copy
import json
import pytest

import qrm_defs.qrm_urls
from qrm_server import management_server
from qrm_defs.resource_definition import Resource


async def test_add_resource(post_to_mgmt_server, redis_db_object, resource_dict_1):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200


async def test_add_two_resources(post_to_mgmt_server, redis_db_object, resource_dict_1, resource_dict_2):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_2]))
    assert resp.status == 200


async def test_resource_added_to_db(post_to_mgmt_server, redis_db_object, resource_dict_1):
    await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    all_resources = await redis_db_object.get_all_resources()
    assert len(all_resources) == 1


async def test_remove_resource(post_to_mgmt_server, redis_db_object, resource_foo, resource_dict_2):
    await redis_db_object.add_resource(resource_foo)
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.REMOVE_RESOURCES, data=json.dumps([resource_dict_2]))
    assert resp.status == 200


async def test_remove_resource_from_db(post_to_mgmt_server, redis_db_object, resource_dict_2,
                                       resource_dict_3):
    await redis_db_object.get_qrm_status()
    await redis_db_object.add_resource(Resource(**resource_dict_2))
    await redis_db_object.get_qrm_status()
    await redis_db_object.add_resource(Resource(**resource_dict_3))
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.REMOVE_RESOURCES, data=json.dumps([resource_dict_2]))
    all_resources = await redis_db_object.get_all_resources()
    assert len(all_resources) == 1


async def test_remove_non_existing_resource_from_db(post_to_mgmt_server, redis_db_object, resource_dict_2,
                                                    resource_dict_3):
    await redis_db_object.add_resource(Resource(**resource_dict_3))
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.REMOVE_RESOURCES, data=json.dumps([resource_dict_2]))
    all_resources = await redis_db_object.get_all_resources()
    assert resp.status == 200
    assert len(all_resources) == 1


async def test_basic_status_empty_db(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('resources_status') == {}


async def test_status_one_resource_with_status(post_to_mgmt_server, redis_db_object, resource_dict_1):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    await redis_db_object.set_resource_status(Resource(**resource_dict_1), status='active')
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'


async def test_status_multiple_resources_with_status(post_to_mgmt_server, redis_db_object, resource_dict_1,
                                                     resource_dict_2):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    await redis_db_object.set_resource_status(Resource(**resource_dict_1), status='active')
    await redis_db_object.add_resource(Resource(**resource_dict_2))
    await redis_db_object.set_resource_status(Resource(**resource_dict_2), status='disabled')
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'
    assert resp_as_dict['resources_status']['resource_2']['status'] == 'disabled'


async def test_status_resource_with_job(post_to_mgmt_server, redis_db_object, resource_dict_1):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    await redis_db_object.set_resource_status(Resource(**resource_dict_1), status='active')
    await redis_db_object.add_job_to_resource(Resource(**resource_dict_1), {'token': 1, 'user': 'foo'})
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict['resources_status']['resource_1']['status'] == 'active'
    assert resp_as_dict['resources_status']['resource_1']['jobs'] == [{'token': 1, 'user': 'foo'}, {}]


async def test_status_qrm_server(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'active'
    await redis_db_object.set_qrm_status('disabled')
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'disabled'


async def test_set_server_status(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_SERVER_STATUS, data=json.dumps({'status': 'disabled'}))
    assert resp.status == 200
    assert await resp.text() == 'mew server status is: disabled\n'


async def test_set_server_status_not_allowed_status(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_SERVER_STATUS, data=json.dumps({'status': 'foo'}))
    assert resp.status == 400
    assert await resp.text() == 'requested status is not allowed: foo\n'


async def test_set_server_status_missing_key_status(post_to_mgmt_server, redis_db_object):
    requested_dict = {'not_status': 'foo'}
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_SERVER_STATUS, data=json.dumps(requested_dict))
    assert resp.status == 400
    assert await resp.text() == f'must specify the status in your request: {requested_dict}\n'


async def test_set_server_status_and_validate_status_output(post_to_mgmt_server, redis_db_object):
    #TODO this test is flaky
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_SERVER_STATUS, data=json.dumps({'status': 'disabled'}))
    assert resp.status == 200
    resp = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'disabled'


@pytest.mark.parametrize('status', ['active', 'disabled'])
async def test_set_resource_status(post_to_mgmt_server, redis_db_object, status, resource_dict_1):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert qrm_status_dict['resources_status']['resource_1']['status'] == ''
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_RESOURCE_STATUS,
                                          data=json.dumps({'status': status, 'resource_name': 'resource_1'}))
    assert resp.status == 200
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert qrm_status_dict['resources_status']['resource_1']['status'] == status


async def test_set_resource_status_resource_not_exist(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_RESOURCE_STATUS,
                                          data=json.dumps({'status': 'active', 'resource_name': 'resource_1'}))
    assert resp.status == 400
    assert await resp.text() == 'Error: resource resource_1 does not exist or status is not allowed\n'


async def test_set_resource_status_missing_key(post_to_mgmt_server, redis_db_object):
    req_dict = {'resource_name': 'resource_1'}
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.SET_RESOURCE_STATUS,
                                          data=json.dumps(req_dict))
    assert resp.status == 400
    assert await resp.text() == f'Error: must specify both status and resource_name in your request: {req_dict}\n'


async def test_add_job_to_resource(post_to_mgmt_server, redis_db_object, resource_dict_1):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    req_dict = {'resource_name': 'resource_1', 'job': {'token': '1', 'job_name': 'foo'}}
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_JOB_TO_RESOURCE,
                                          data=json.dumps(req_dict))
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [req_dict['job'], {}]


async def test_remove_job_from_resource(post_to_mgmt_server, redis_db_object, resource_dict_1):
    await redis_db_object.add_resource(Resource(**resource_dict_1))
    req_dict = {'resource_name': 'resource_1', 'job': {'token': '1', 'job_name': 'foo'}}
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_JOB_TO_RESOURCE,
                                          data=json.dumps(req_dict))
    assert resp.status == 200
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [req_dict['job'], {}]
    await post_to_mgmt_server.post(qrm_defs.qrm_urls.REMOVE_JOB,
                                   data=json.dumps({'token': '1', 'resources': ['resource_1']}))
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert resp.status == 200
    assert qrm_status_dict['resources_status']['resource_1']['jobs'] == [{}]


async def test_add_existing_resource(post_to_mgmt_server, redis_db_object, resource_dict_1):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    assert await resp.text() == 'didn\'t add any resource, check if the resource already exists\n'


async def test_add_existing_resource_remove_add_again(post_to_mgmt_server, redis_db_object, resource_dict_1):
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.REMOVE_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    resp = await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([resource_dict_1]))
    assert resp.status == 200
    assert 'added the following resources' in await resp.text()


async def test_basic_token_grouping(post_to_mgmt_server, redis_db_object, resource_dict_1, resource_dict_2,
                                    resource_dict_3):
    res1_with_token = copy.deepcopy(resource_dict_1)
    res1_with_token['token'] = 'token1'
    res2_with_token = copy.deepcopy(resource_dict_2)
    res2_with_token['token'] = 'token1'
    res3_with_token = copy.deepcopy(resource_dict_3)
    res3_with_token['token'] = 'token2'
    await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([res1_with_token]))
    await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([res2_with_token]))
    await post_to_mgmt_server.post(qrm_defs.qrm_urls.ADD_RESOURCES, data=json.dumps([res3_with_token]))
    qrm_status = await post_to_mgmt_server.get(qrm_defs.qrm_urls.MGMT_STATUS_API)
    qrm_status_dict = await qrm_status.json()
    assert qrm_status.status == 200
    assert {res1_with_token['name']: res1_with_token['type']} in qrm_status_dict['groups']['token1']
    assert {res2_with_token['name']: res2_with_token['type']} in qrm_status_dict['groups']['token1']
    assert [{res3_with_token['name']: res3_with_token['type']}] == qrm_status_dict['groups']['token2']
