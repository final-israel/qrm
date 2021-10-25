import asyncio
import time

import pytest
import subprocess
import qrm_db

from redis_adapter import RedisDB


def test_env():
    # validate that redis-server is installed
    try:
        subprocess.Popen(['/usr/bin/redis-server', '--version'])
    except Exception as e:
        pytest.fail('can\'t find redis-server app installation, please run:\nsudo apt install redis-server')


@pytest.mark.asyncio
async def test_add_resource(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    assert 1 == len(await redis_db_object.get_all_keys_by_pattern('*foo'))


@pytest.mark.asyncio
async def test_remove_resource(redis_db_object):
    """Check that it's actually working on redis database."""
    await redis_db_object.add_resource('test1')
    await redis_db_object.remove_resource(resource_name='test1')
    assert qrm_db.get_resource_name_in_db('test_1') not in await redis_db_object.get_all_keys_by_pattern('*')


@pytest.mark.asyncio
async def test_get_all_keys_by_pattern(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    await redis_db_object.add_resource(resource_name='bar')
    result1 = await redis_db_object.get_all_keys_by_pattern(pattern='*foo')
    result2 = await redis_db_object.get_all_keys_by_pattern('*')
    assert [qrm_db.get_resource_name_in_db('foo')] == result1
    assert qrm_db.get_resource_name_in_db('foo') in result2
    assert qrm_db.get_resource_name_in_db('bar') in result2


@pytest.mark.asyncio
async def test_get_all_resources(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    await redis_db_object.add_resource(resource_name='bar')
    result = await redis_db_object.get_all_resources()
    assert 2 == len(result)
    assert f'{qrm_db.get_resource_name_in_db("foo")}' in result


@pytest.mark.asyncio
async def test_set_and_get_server_status(redis_db_object):
    await redis_db_object.set_resource_status(resource_name='foo', status='active')
    assert await redis_db_object.get_resource_status(resource_name='foo') == 'active'
    await redis_db_object.set_resource_status(resource_name='foo', status='not_available')
    assert await redis_db_object.get_resource_status(resource_name='foo') == 'not_available'


def test_build_resource_jobs_as_dicts():
    jobs_list = ['{"id": 1, "user": "bar"}', '{}']
    expected_ret = [{'id': 1, 'user': 'bar'}, {}]
    assert expected_ret == RedisDB.build_resource_jobs_as_dicts(jobs_list=jobs_list)


@pytest.mark.asyncio
async def test_add_job_to_resource(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    await redis_db_object.add_job_to_resource(resource_name='foo', job={'id': 1, 'user': 'bar'})
    jobs = await redis_db_object.get_resource_jobs(resource_name='foo')
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
