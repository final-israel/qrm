import copy
import json
import time
import pytest
import subprocess

from redis_adapter import RedisDB
from qrm_server.resource_definition import Resource


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
    jobs_list = ['{"id": 1, "user": "bar"}', '{}']
    expected_ret = [{'id': 1, 'user': 'bar'}, {}]
    assert expected_ret == RedisDB.build_resource_jobs_as_dicts(jobs_list=jobs_list)


@pytest.mark.asyncio
async def test_add_job_to_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_job_to_resource(resource_foo, job={'id': 1, 'user': 'bar'})
    jobs = await redis_db_object.get_resource_jobs(resource_foo)
    assert jobs == [{'id': 1, 'user': 'bar'}, {}]


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
    job1 = {'id': 1, 'user': 'bar'}
    job2 = {'id': 2, 'user': 'xxx'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_foo, job=job2)
    assert job1 and job2 in await redis_db_object.get_resource_jobs(resource_foo)
    ret_job_1 = await redis_db_object.get_job_for_resource_by_id(resource_foo, job_id=1)
    ret_job_2 = await redis_db_object.get_job_for_resource_by_id(resource_foo, job_id=2)
    assert json.loads(ret_job_1) == job1
    assert json.loads(ret_job_2) == job2


@pytest.mark.asyncio
async def test_remove_job_from_one_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    job1 = {'id': 1, 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    await redis_db_object.remove_job(job_id=1, resources_list=[resource_foo])
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)


@pytest.mark.asyncio
async def test_remove_job_from_multiple_resources(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    resource_aaa = Resource(name='aaa', type='server')
    await redis_db_object.add_resource(resource_aaa)
    job1 = {'id': 1, 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_bar, job=job1)
    await redis_db_object.add_job_to_resource(resource_aaa, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 in await redis_db_object.get_resource_jobs(resource_bar)
    assert job1 in await redis_db_object.get_resource_jobs(resource_aaa)
    await redis_db_object.remove_job(job_id=1, resources_list=[resource_foo, resource_bar])
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 not in await redis_db_object.get_resource_jobs(resource_bar)
    assert job1 in await redis_db_object.get_resource_jobs(resource_aaa)


@pytest.mark.asyncio
async def test_remove_job_from_all_resources_in_db(redis_db_object, resource_foo, resource_bar):
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    job1 = {'id': 1, 'user': 'bar'}
    await redis_db_object.add_job_to_resource(resource_foo, job=job1)
    await redis_db_object.add_job_to_resource(resource_bar, job=job1)
    assert job1 in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 in await redis_db_object.get_resource_jobs(resource_bar)
    # in this case the job should be removed from all resources queue:
    await redis_db_object.remove_job(job_id=1)
    assert job1 not in await redis_db_object.get_resource_jobs(resource_foo)
    assert job1 not in await redis_db_object.get_resource_jobs(resource_bar)


@pytest.mark.asyncio
async def test_add_existing_resource(redis_db_object, resource_foo):
    await redis_db_object.add_resource(resource_foo)
    assert not await redis_db_object.add_resource(resource_foo)


@pytest.mark.asyncio
async def test_equal_operator_resource(redis_db_object, resource_foo):
    resource_bar = copy.deepcopy(resource_foo)
    assert resource_bar == resource_foo
    resource_bar.status = 'something'
    assert resource_bar == resource_foo
    resource_bar.name = 'different_name'
    assert not resource_foo == resource_bar
