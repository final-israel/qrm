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
