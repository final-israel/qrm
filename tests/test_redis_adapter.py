import copy
import json
import time
import pytest
import subprocess

from db_adapters.redis_adapter import RedisDB
from qrm_resources.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, \
    generate_token_from_seed


def test_env():
    # validate that redis-server is installed
    try:
        subprocess.Popen(['/usr/bin/redis-server', '--version'])
    except Exception as e:
        pytest.fail('can\'t find redis-server app installation, please run:\nsudo apt install redis-server')


@pytest.mark.asyncio
async def test_add_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    assert 1 == len(await redis_db_object.get_all_keys_by_pattern('*foo'))


@pytest.mark.asyncio
async def test_remove_resource(redis_db_object, resource_foo):
    """Check that it's actually working on redis database."""
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.remove_resource(resource_foo)
    assert resource_foo.db_name() not in await redis_db_object.get_all_keys_by_pattern('*')


@pytest.mark.asyncio
async def test_get_all_keys_by_pattern(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    result1 = await redis_db_object.get_all_keys_by_pattern(pattern='*foo')
    result2 = await redis_db_object.get_all_keys_by_pattern('*')
    assert [resource_foo.db_name()] == result1
    assert resource_foo.db_name() in result2
    assert resource_bar.db_name() in result2


@pytest.mark.asyncio
async def test_get_all_resources(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    result = await redis_db_object.get_all_resources()
    assert len(result) == 2
    assert resource_foo in result
    assert resource_bar in result


@pytest.mark.asyncio
async def test_set_and_get_server_status(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.set_resource_status(resource_foo, status='active')
    assert await redis_db_object.get_resource_status(resource_foo) == 'active'
    await redis_db_object.set_resource_status(resource_foo, status='not_available')
    assert await redis_db_object.get_resource_status(resource_foo) == 'not_available'


def test_build_resource_jobs_as_dicts():
    jobs_list = ['{"token": 1, "user": "bar"}', '{}']
    expected_ret = [{'token': 1, 'user': 'bar'}, {}]
    assert expected_ret == RedisDB.build_resource_jobs_as_dicts(jobs_list=jobs_list)


@pytest.mark.asyncio
async def test_add_job_to_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_job_to_resource(resource_foo, job={'token': 1, 'user': 'bar'})
    jobs = await redis_db_object.get_resource_jobs(resource_foo)
    assert jobs == [{'token': 1, 'user': 'bar'}, {}]


@pytest.mark.asyncio
async def test_set_qrm_status(redis_db_object):
    time.sleep(0.1)
    await redis_db_object.set_qrm_status(status='disabled')
    time.sleep(0.1)
    assert 'disabled' == await redis_db_object.get_qrm_status()


@pytest.mark.asyncio
async def test_server_status_default_value(redis_db_object):
    time.sleep(0.1)
    assert 'active' == await redis_db_object.get_qrm_status()


@pytest.mark.asyncio
async def test_set_qrm_status_not_allowed_status(redis_db_object):
    assert not await redis_db_object.set_qrm_status(status='foo')
    time.sleep(0.1)
    assert 'active' == await redis_db_object.get_qrm_status()


@pytest.mark.asyncio
async def test_get_resource_matched_job_by_id(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    job1 = {'token': '1', 'user': 'bar'}
    job2 = {'token': '2', 'user': 'xxx'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_foo, job=job2)
    assert job1 and job2 in await redis_db_object.get_resource_jobs(resource_foo)
    ret_job_1 = await redis_db_object.get_job_for_resource_by_id(resource_foo, token=job1['token'])
    ret_job_2 = await redis_db_object.get_job_for_resource_by_id(resource_foo, token=job2['token'])
    assert json.loads(ret_job_1) == job1
    assert json.loads(ret_job_2) == job2


@pytest.mark.asyncio
async def test_remove_job_from_one_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    job1 = {'token': '1', 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    await redis_db_object.remove_job(token='1', resources_list=[resource_foo])
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)


@pytest.mark.asyncio
async def test_remove_job_from_multiple_resources(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    resource_aaa = Resource(name='aaa', type='server')
    await redis_db_object.add_resource(resource_aaa)
    job1 = {'token': '1', 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_bar, job=job1)
    await redis_db_object.add_job_to_resource(resource_aaa, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 in await redis_db_object.get_resource_jobs(resource_bar)
    assert job1 in await redis_db_object.get_resource_jobs(resource_aaa)
    await redis_db_object.remove_job(token='1', resources_list=[resource_foo, resource_bar])
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 not in await redis_db_object.get_resource_jobs(resource_bar)
    assert job1 in await redis_db_object.get_resource_jobs(resource_aaa)


@pytest.mark.asyncio
async def test_remove_job_from_all_resources_in_db(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    job1 = {'token': '1', 'user': 'bar'}
    job2 = {'token': '2'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_foo, job=job2)
    await redis_db_object.add_job_to_resource(resource_bar, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 in await redis_db_object.get_resource_jobs(resource_bar)
    # in this case the job should be removed from all resources queue:
    await redis_db_object.remove_job(token='1')
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 not in await redis_db_object.get_resource_jobs(resource_bar)
    await redis_db_object.remove_job(token='2')
    assert job2 not in await redis_db_object.get_resource_jobs(resource_foo)


@pytest.mark.asyncio
async def test_add_existing_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    assert not await redis_db_object.add_resource(resource_foo)


def test_equal_operator_resource(redis_db_object, resource_foo):
    resource_bar = copy.deepcopy(resource_foo)
    assert resource_bar == resource_foo
    resource_bar.status = 'something'
    assert resource_bar == resource_foo
    resource_bar.name = 'different_name'
    assert not resource_foo == resource_bar
    assert not resource_foo == {}


def test_resources_request():
    # This usage is OK:
    ResourcesRequest(token='abc')
    # two from a,b or c, and one from f, which is actually f:
    req = ResourcesRequest()
    req.add_request_by_tags(tags=['a', 'b'], count=2)
    assert len(req.as_dict()['tags']) == 1
    req.add_request_by_names(names=['name1', 'name2'], count=1)
    assert len(req.as_dict()['names']) == 1


@pytest.mark.asyncio
async def test_set_get_token(redis_db_object, resource_foo, resource_bar):
    resource_foo.token = '123'
    resource_bar.token = '123'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    await redis_db_object.generate_token('123', [resource_foo, resource_bar])
    token_list_in_db = await redis_db_object.get_token_resources('123')
    assert type(token_list_in_db) == list
    assert len(token_list_in_db) == 2
    assert resource_foo in token_list_in_db
    assert resource_bar in token_list_in_db


@pytest.mark.asyncio
async def test_generate_exists_token(redis_db_object, resource_foo, resource_bar):
    resource_foo.token = 'test_set_exists_token'
    resource_bar.token = 'test_set_exists_token'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    assert await redis_db_object.generate_token('test_set_exists_token', [resource_foo, resource_bar])
    assert not await redis_db_object.generate_token('test_set_exists_token', [resource_foo, resource_bar])


@pytest.mark.asyncio
async def test_token_not_in_db(redis_db_object):
    assert not await redis_db_object.get_token_resources('non_existing_token')


@pytest.mark.asyncio
async def test_add_resources_request(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    open_requests = await redis_db_object.get_open_requests()
    assert req_token in open_requests
    assert res_req == open_requests[req_token]


@pytest.mark.asyncio
async def test_get_resources_request_by_token(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    open_request = await redis_db_object.get_open_request_by_token(req_token)
    assert res_req == open_request


@pytest.mark.asyncio
async def test_get_resources_request_by_token_which_expired(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    open_request = await redis_db_object.get_open_request_by_token('other_token')
    assert open_request == ResourcesRequest()


@pytest.mark.asyncio
async def test_update_open_request(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    open_request = await redis_db_object.get_open_request_by_token(req_token)
    assert len(open_request.names) == 1
    res_req.names.pop(0)
    await redis_db_object.update_open_request(req_token, res_req)
    open_request = await redis_db_object.get_open_request_by_token(req_token)
    assert len(open_request.names) == 0


@pytest.mark.asyncio
async def test_update_open_request_when_request_not_exist(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    res_req = ResourcesRequest()
    assert not await redis_db_object.update_open_request(req_token, res_req)


@pytest.mark.asyncio
async def test_remove_open_request(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    await redis_db_object.remove_open_request(req_token)
    assert ResourcesRequest() == await redis_db_object.get_open_request_by_token(req_token)


@pytest.mark.asyncio
async def test_remove_open_request_not_exists_request(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.add_resources_request(res_req)
    await redis_db_object.remove_open_request('other_token')
    assert res_req == await redis_db_object.get_open_request_by_token(req_token)


@pytest.mark.asyncio
async def test_add_partially_fill_request(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.partial_fill_request(req_token, resource_foo)
    assert await redis_db_object.get_partial_fill(req_token) == ResourcesRequestResponse([resource_foo.name], req_token)


@pytest.mark.asyncio
async def test_remove_partially_fill_requset(redis_db_object, resource_foo, resource_bar):
    req_token = '123456'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    res_req = ResourcesRequest()
    res_req.add_request_by_token(req_token)
    res_req.add_request_by_names(names=[resource_foo.name, resource_bar.name], count=1)
    await redis_db_object.partial_fill_request(req_token, resource_foo)
    assert await redis_db_object.get_partial_fill(req_token) == ResourcesRequestResponse([resource_foo.name], req_token)
    await redis_db_object.remove_partially_fill_request(req_token)
    assert await redis_db_object.get_partial_fill(token=req_token) == ResourcesRequestResponse()


@pytest.mark.asyncio
async def test_get_resource_by_name(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    assert resource_foo == await redis_db_object.get_resource_by_name(resource_foo.name)


@pytest.mark.asyncio
async def test_get_resource_by_name_resource_not_exist(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    assert resource_foo == await redis_db_object.get_resource_by_name(resource_foo.name)
    assert await redis_db_object.get_resource_by_name('other_resource') is None


@pytest.mark.asyncio
async def test_get_active_job(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    job1 = {'token': '1', 'user': 'bar'}
    job2 = {'token': '2', 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_foo, job=job2)
    assert job1 == await redis_db_object.get_active_job(resource_foo)
    await redis_db_object.remove_job(job1['token'], [resource_foo])
    assert job2 == await redis_db_object.get_active_job(resource_foo)


@pytest.mark.asyncio
async def test_get_active_job_multiple_resources(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    job1 = {'token': '1', 'user': 'bar'}
    job2 = {'token': '2', 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job1)
    await redis_db_object.add_job_to_resource(resource_foo, job2)
    await redis_db_object.add_job_to_resource(resource_bar, job2)
    await redis_db_object.add_job_to_resource(resource_bar, job1)
    assert job1 == await redis_db_object.get_active_job(resource_foo)
    assert job2 == await redis_db_object.get_active_job(resource_bar)
    await redis_db_object.remove_job(job1['token'])
    assert job2 == await redis_db_object.get_active_job(resource_bar)
    assert job2 == await redis_db_object.get_active_job(resource_foo)
    await redis_db_object.remove_job(job2['token'])
    assert {} == await redis_db_object.get_active_job(resource_bar)
    assert {} == await redis_db_object.get_active_job(resource_foo)


@pytest.mark.asyncio
async def test_set_token_for_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.set_token_for_resource('unique_token', resource_foo)
    res_from_db = await redis_db_object.get_resource_by_name(resource_foo.name)
    assert 'unique_token' == res_from_db.token


@pytest.mark.asyncio
async def test_get_resources_by_names(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    assert [resource_foo] == await redis_db_object.get_resources_by_names([resource_foo.name])
    assert resource_foo and resource_bar in await \
        redis_db_object.get_resources_by_names([resource_foo.name, resource_bar.name])


@pytest.mark.asyncio
async def test_get_resources_by_names_resource_not_in_db(redis_db_object, resource_foo):
    assert [] == await redis_db_object.get_resources_by_names([resource_foo.name])


@pytest.mark.asyncio
async def test_is_request_filled(redis_db_object, resource_foo):
    token = 'test_token'
    assert not await redis_db_object.is_request_filled(token=token)
    await redis_db_object.generate_token(token, [resource_foo])
    assert await redis_db_object.is_request_filled(token=token)


def test_generate_seed_basic():
    seed = 'basic_token'
    token = generate_token_from_seed(seed)
    assert len(token.split('_')) == 8


def test_generate_token_from_existing_one():
    token = 'basic_token_2022_01_14_07_53_28'
    new_token = generate_token_from_seed(token)
    assert len(new_token.split('_')) == 8
    assert token != new_token


def test_generate_token_long_name_special_chars():
    token = 'just-a-long_name-with$pecial*chars&_2022_01_14_07_53_28'
    new_token = generate_token_from_seed(token)
    assert len(new_token.split('_')) == 8
    assert token != new_token


@pytest.mark.asyncio
async def test_user_token_active_token_mapping(redis_db_object):
    user_token = "123"
    active_token = "456"

    await redis_db_object.set_active_token_for_user_token(user_token, active_token)
    res = await redis_db_object.get_active_token_from_user_token(user_token)

    assert res == active_token


@pytest.mark.asyncio
async def test_user_token_active_token_nonexisting_mapping(redis_db_object):
    user_token = "123"
    res = await redis_db_object.get_active_token_from_user_token(user_token)

    assert res is None


@pytest.mark.asyncio
async def test_res_status_change_event_after_recovery(resource_foo, redis_db_object):
    redis_obj = RedisDB()
    await redis_obj.add_resource(resource_foo)
    assert redis_obj.res_status_change_event[resource_foo.name]
    del redis_obj
    new_redis_obj = RedisDB()
    all_res = await new_redis_obj.get_all_resources()
    await new_redis_obj.init_default_params()
    assert new_redis_obj.res_status_change_event[resource_foo.name]
