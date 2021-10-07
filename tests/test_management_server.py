import json
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
    await redis_db_object.add_resource(resource_name='resource_2')
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
    await redis_db_object.set_resource_status(resource_name='resource_2', status='disabled')
    await redis_db_object.add_resource(resource_name='resource_2')
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
    assert resp.reason == 'mew server status is: disabled'


async def test_set_server_status_not_allowed_status(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps({'status': 'foo'}))
    assert resp.status == 400
    assert resp.reason == 'requested status is not allowed: foo'


async def test_set_server_status_missing_key_status(post_to_mgmt_server, redis_db_object):
    requested_dict = {'not_status': 'foo'}
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps(requested_dict))
    assert resp.status == 400
    assert resp.reason == f'must specify the status in your request: {requested_dict}'


async def test_set_server_status_and_validate_status_output(post_to_mgmt_server, redis_db_object):
    resp = await post_to_mgmt_server.post(management_server.SET_SERVER_STATUS, data=json.dumps({'status': 'disabled'}))
    assert resp.status == 200
    resp = await post_to_mgmt_server.get(management_server.STATUS)
    resp_as_dict = await resp.json()
    assert resp.status == 200
    assert resp_as_dict.get('qrm_server_status') == 'disabled'
