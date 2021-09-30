import pytest
import subprocess
from db_adapters import redis_adapter


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
    assert [] == await redis_db_object.get_all_keys_by_pattern('*')


@pytest.mark.asyncio
async def test_get_all_keys_by_pattern(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    await redis_db_object.add_resource(resource_name='bar')
    result1 = await redis_db_object.get_all_keys_by_pattern(pattern='*foo')
    result2 = await redis_db_object.get_all_keys_by_pattern('*')
    assert ['resource_name_foo'] == result1
    assert 2 == len(result2)


@pytest.mark.asyncio
async def test_get_all_resources(redis_db_object):
    await redis_db_object.add_resource(resource_name='foo')
    await redis_db_object.add_resource(resource_name='bar')
    result = await redis_db_object.get_all_resources()
    assert 2 == len(result)
    assert f'{redis_adapter.PREFIX_NAME}_foo' in result



