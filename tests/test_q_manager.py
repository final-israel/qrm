import asyncio
import logging
import pytest
import time

from qrm_defs.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName, \
    PENDING_STATUS, ACTIVE_STATUS, DISABLED_STATUS, ResourcesByTags
from qrm_server.q_manager import QueueManagerBackEnd
from db_adapters.redis_adapter import RedisDB
from typing import List


@pytest.mark.asyncio
async def test_qbackend_new_request_by_token_only(redis_db_object, qrm_backend_with_db):
    req_token = 'my_req_token'
    res_1 = Resource(name='res1', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_3 = Resource(name='res3', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_4 = Resource(name='res4', type='type1', token='my_req_other_token', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)
    await redis_db_object.generate_token(req_token, [res_1, res_2, res_3])
    user_request = ResourcesRequest()
    user_request.add_request_by_token(req_token)
    response = await qrm_backend_with_db.new_request(resources_request=user_request)
    assert req_token == response.token
    assert len(response.names) == 3
    assert res_1.name in response.names
    assert res_2.name in response.names
    assert res_3.name in response.names
    assert res_4.name not in response.names
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_qbackend_2_requests_same_time(redis_db_object, qrm_backend_with_db, qrm_management_server):
    req_token = 'my_req_token'
    res_1 = Resource(name='server1', type='server', token='old_token1', status=ACTIVE_STATUS, tags=['server'])
    res_2 = Resource(name='server2', type='server', token='old_token2', status=ACTIVE_STATUS, tags=['server'])
    res_3 = Resource(name='server3', type='server', token='old_token3', status=ACTIVE_STATUS, tags=['server'])
    res_4 = Resource(name='vlan1', type='vlan', token='old_token1', status=ACTIVE_STATUS, tags=['vlan'])
    res_5 = Resource(name='vlan2', type='vlan', token='old_token2', status=ACTIVE_STATUS, tags=['vlan'])
    res_6 = Resource(name='vlan3', type='vlan', token='old_token3', status=ACTIVE_STATUS, tags=['vlan'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)
    await redis_db_object.add_resource(res_5)
    await redis_db_object.add_resource(res_6)

    user_request1 = ResourcesRequest(token='test1')
    user_request1.add_request_by_tags(tags=['vlan'], count=1)
    user_request1.add_request_by_tags(tags=['server'], count=1)

    user_request2 = ResourcesRequest(token='test2')
    user_request2.add_request_by_tags(tags=['vlan'], count=1)
    user_request2.add_request_by_tags(tags=['server'], count=1)

    response = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request1))
    response2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request2))

    await response
    await response2

    token1 = await qrm_backend_with_db.get_new_token('test1')
    token2 = await qrm_backend_with_db.get_new_token('test2')
    await asyncio.sleep(0.1)

    resp1 = await qrm_backend_with_db.get_resource_req_resp(token1)
    resp2 = await qrm_backend_with_db.get_resource_req_resp(token2)

    logging.info(f"resp1: {resp1}")
    logging.info(f"resp2: {resp2}")

    assert len(resp1.names) == 2
    assert len(resp2.names) == 2

    await redis_db_object.close()


@pytest.mark.asyncio
async def test_request_by_token_not_valid(redis_db_object, qrm_backend_with_db):
    req_token = 'other_token'
    res_1 = Resource(name='res1', type='type1', token=req_token)
    res_2 = Resource(name='res2', type='type1', token=req_token)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.generate_token(req_token, [res_1, res_2])
    user_request = ResourcesRequest()
    req_token = 'my_req_token'
    user_request.add_request_by_token(req_token)
    await qrm_backend_with_db.new_request(resources_request=user_request)
    new_token = await qrm_backend_with_db.get_new_token(req_token)
    response = await qrm_backend_with_db.get_resource_req_resp(new_token)

    assert new_token in response.token
    assert 'contains names and tags' in response.message
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_request_reorder_names_request(redis_db_object, qrm_backend_with_db):
    old_token = 'old_token'
    res_1 = Resource(name='res1', type='type1', token=old_token)
    res_2 = Resource(name='res2', type='type1', token=old_token)
    res_3 = Resource(name='res3', type='type1', token='other_token')
    res_4 = Resource(name='res4', type='type1', token=old_token)
    res_5 = Resource(name='res5', type='type1', token=old_token)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)
    await redis_db_object.add_resource(res_5)
    await redis_db_object.generate_token(old_token, [res_1, res_2, res_3, res_4, res_5])
    names_request = ResourcesByName(names=[res_1.name, res_2.name, res_3.name, res_4.name, res_5.name], count=2)
    all_resources_dict = await redis_db_object.get_all_resources_dict()
    await qrm_backend_with_db.reorder_names_request(old_token, [names_request], all_resources_dict=all_resources_dict)
    assert names_request.names[-1] == res_3.name
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_request_reorder_names_resource_not_in_db(redis_db_object, qrm_backend_with_db):
    # in this case the method should remove the resource from the request:
    old_token = 'old_token'
    res_1 = Resource(name='res1', type='type1', token=old_token)
    res_2 = Resource(name='res2', type='type1', token=old_token)
    res_3 = Resource(name='res3', type='type1', token='other_token')
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.generate_token(old_token, [res_1, res_2, res_3])
    # remove res_2 from db:
    await redis_db_object.remove_resource(res_2)
    names_request = ResourcesByName(names=[res_1.name, res_2.name, res_3.name], count=2)
    all_resources_dict = await redis_db_object.get_all_resources_dict()
    await qrm_backend_with_db.reorder_names_request(old_token, [names_request], all_resources_dict=all_resources_dict)
    assert names_request.names[0] == res_1.name
    assert names_request.names[-1] == res_3.name
    assert res_2.name not in names_request.names
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_request_reorder_names_request_multiple_requestes(redis_db_object, qrm_backend_with_db):
    old_token = 'token1'
    res_1 = Resource(name='res1', type='type1', token=old_token)
    res_2 = Resource(name='res2', type='type1', token='other_token')
    res_3 = Resource(name='res3', type='type1', token=old_token)
    res_4 = Resource(name='res4', type='type1', token='other_token')
    res_5 = Resource(name='res5', type='type1', token=old_token)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)
    await redis_db_object.add_resource(res_5)
    await redis_db_object.generate_token(old_token, [res_1, res_2, res_3, res_4, res_5])
    names_request1 = ResourcesByName(names=[res_1.name, res_2.name, res_3.name], count=2)
    names_request2 = ResourcesByName(names=[res_4.name, res_5.name], count=2)
    all_resources_dict = await redis_db_object.get_all_resources_dict()
    await qrm_backend_with_db.reorder_names_request(old_token, [names_request1, names_request2],
                                                    all_resources_dict=all_resources_dict)
    assert names_request1.names[-1] == res_2.name
    assert names_request2.names[-1] == res_4.name
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_names_worker_basic_request(qrm_backend_with_db, redis_db_object):
    token = 'token1'
    job1 = {'token': token, 'user': 'bar'}
    job2 = {'token': 'other_token', 'user': 'bar'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1
    await redis_db_object.add_job_to_resource(res_1, job=job2)
    await redis_db_object.add_job_to_resource(res_1, job=job1)
    await redis_db_object.add_job_to_resource(res_2, job=job1)
    # we want both res_1 and res_2:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(token)
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    await redis_db_object.add_resources_request(user_request)
    await qrm_backend_with_db.init_event_for_token(token)
    response = qrm_backend_with_db.names_worker(token)
    # token is not completed since the work in progress until job2 will be removed from res_1:
    assert not await redis_db_object.is_request_filled(token)
    await redis_db_object.remove_job('other_token')
    assert await response == ResourcesRequestResponse(names=['res1', 'res2'], token=token)
    assert await redis_db_object.is_request_filled(token)
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_request_by_names(redis_db_object, qrm_backend_with_db):
    token = 'token1'
    job1 = {'token': token}
    job2 = {'token': 'other_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_job_to_resource(res_1, job=job2)
    # we want both res_1 and res_2:
    user_request = ResourcesRequest()  # job1
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)

    task = asyncio.ensure_future(remove_job_and_set_event_after_timeout(0.1, token, qrm_backend_with_db,
                                                                        redis_db_object, 'other_token'))
    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(token)
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert res_1.name in result.names
    assert res_2.name in result.names
    assert len(result.names) == 2
    await redis_db_object.close()

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_cancel_request(redis_db_object, qrm_backend_with_db):
    async def _cancel_request(timeout_sec: float, token: str):
        await asyncio.sleep(timeout_sec)
        await qrm_backend_with_db.cancel_request(token)

    def cancel_cb(result):
        try:
            ret = result.result()
        except Exception:
            # TODO: make it less ugly
            asyncio.get_event_loop().stop()
            raise Exception("dead")

        return ret

    token = 'token1'
    job1 = {'token': token}
    job2 = {'token': 'other_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)

    await qrm_backend_with_db.new_request(user_request)

    # we want both res_1 and res_2:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1

    t = 0.1
    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2["token"])
    fut = asyncio.ensure_future(_cancel_request(t, active_token_job_2))
    fut.add_done_callback(cancel_cb)
    result = await asyncio.wait_for(
        qrm_backend_with_db.new_request(user_request),
        timeout=(t * 2)
    )
    assert res_1.name in result.names
    assert res_2.name in result.names
    assert len(result.names) == 2
    await redis_db_object.close()


@pytest.mark.asyncio
async def test_is_request_active(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    await qrm_backend_with_db.new_request(user_request)

    # we want both res_1 and res_2:
    user_request = ResourcesRequest()  # job1 request should be active at this
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    result_job_1 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1
    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    # request should be active at this point:
    assert await qrm_backend_with_db.is_request_active(active_token_job_1)
    await qrm_backend_with_db.cancel_request(active_token_job_2)
    result_job_1 = await result_job_1
    # after result received, the request is no longer active:
    assert not await qrm_backend_with_db.is_request_active(active_token_job_1)
    assert res_1.name in result_job_1.names
    assert res_2.name in result_job_1.names
    assert len(result_job_1.names) == 2


@pytest.mark.asyncio
async def test_is_request_active_after_cancel(redis_db_object, qrm_backend_with_db):
    token = 'token1'
    job1 = {'token': token}
    job2 = {'token': 'other_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # we want both res_1 and res_2:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    await qrm_backend_with_db.new_request(user_request)

    user_request = ResourcesRequest()  # job1
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    # at this point the new request is running in BG and still active

    new_active_token = await qrm_backend_with_db.get_new_token(token)
    assert await qrm_backend_with_db.is_request_active(new_active_token)
    await qrm_backend_with_db.cancel_request(new_active_token)
    assert not await qrm_backend_with_db.is_request_active(new_active_token)

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_get_filled_request(redis_db_object, qrm_backend_with_db):
    token = 'token1'
    job1 = {'token': token}
    job2 = {'token': 'other_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # we want both res_1 and res_2:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    await qrm_backend_with_db.new_request(user_request)

    user_request = ResourcesRequest()  # job1
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_2)
    while await qrm_backend_with_db.is_request_active(active_token_job_1):
        await asyncio.sleep(0.05)
    result = await qrm_backend_with_db.get_resource_req_resp(active_token_job_1)
    assert res_1.name in result.names
    assert res_2.name in result.names
    assert len(result.names) == 2

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_validation_count_larger_than_requested_resources(redis_db_object, qrm_backend_with_db):
    # count > len(resources)
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    with pytest.raises(Exception):
        assert not user_request.add_request_by_names([res_1.name], count=2)


@pytest.mark.asyncio
async def test_multiple_jobs_in_queue(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    job3 = {'token': 'job_3_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    await qrm_backend_with_db.new_request(user_request)

    # add job3 to res_1 and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job3["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    res_job_3 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    active_token_job_3 = await qrm_backend_with_db.get_new_token(job3['token'])
    while not await qrm_backend_with_db.is_request_active(active_token_job_3):
        await asyncio.sleep(0.1)

    # add job1 to both res_1 and res_2 queues
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    res_job_1 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    # queues at this point: res1: [job_1, job_3, job_2], res_2: [job_1, job_3]
    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    # requests still active at this point:
    assert await qrm_backend_with_db.is_request_active(active_token_job_1)
    assert await qrm_backend_with_db.is_request_active(active_token_job_3)

    # cancel job2, job3 should get response now:
    await qrm_backend_with_db.cancel_request(active_token_job_2)
    while await qrm_backend_with_db.is_request_active(active_token_job_3):
        await asyncio.sleep(0.1)
    res_job_3 = await res_job_3
    assert res_1.name and res_2.name in res_job_3.names
    assert not await qrm_backend_with_db.is_request_active(active_token_job_3)

    # cancel job3, job1 should get response now:
    assert await qrm_backend_with_db.is_request_active(active_token_job_1)
    await qrm_backend_with_db.cancel_request(active_token_job_3)
    res_job_1 = await res_job_1
    assert res_1.name and res_2.name in res_job_1.names
    assert not await qrm_backend_with_db.is_request_active(active_token_job_1)


@pytest.mark.asyncio
async def test_multiple_resources_request_same_request(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    res_3 = Resource(name='res3', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)

    # add job1 to res_1, res_2 queue and res_3 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    user_request.add_request_by_names([res_3.name], count=1)

    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name and res_2.name and res_3.name in result.names


@pytest.mark.asyncio
async def test_requested_resources_larger_than_count(qrm_backend_with_db, redis_db_object):
    # this test simulates the case where len(resources.names) > count
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    res_3 = Resource(name='res3', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    await qrm_backend_with_db.new_request(user_request)

    # resources queus: res_1: [job2], res_2: [], res_3: []
    # therefore the next request should go to res_2 and res_3
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name, res_3.name], count=2)
    result = await qrm_backend_with_db.new_request(user_request)

    assert res_2.name and res_3.name in result.names
    assert len(result.names) == 2


@pytest.mark.asyncio
async def test_cancel_job_waiting_in_queue(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    job3 = {'token': 'job_3_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    await qrm_backend_with_db.new_request(user_request)

    # add job2 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2["token"])
    while not await qrm_backend_with_db.is_request_active(active_token_job_2):
        await asyncio.sleep(0.1)  # just waiting for the job to be active

    # add job3 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job3["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    res_job_3 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    active_token_job_3 = await qrm_backend_with_db.get_new_token(job3["token"])
    while not await qrm_backend_with_db.is_request_active(active_token_job_3):
        await asyncio.sleep(0.1)  # just waiting for the job to be active

    await qrm_backend_with_db.cancel_request(active_token_job_2)
    assert await qrm_backend_with_db.is_request_active(active_token_job_3)

    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_1)

    # now after both job1 and job 2 were cancelled, job 3 should be filled:
    res_job_3 = await res_job_3
    assert res_1.name and res_2.name in res_job_3.names

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_recovery_jobs_in_queue(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    await add_token_to_resources(qrm_backend_with_db, [res_1])

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    assert await qrm_backend_with_db.is_request_active(new_token_job_1)

    await cancel_all_open_tasks([task])

    # remove the old QrmBackend and init a new instance:
    await qrm_backend_with_db.stop_backend()
    new_qrm = QueueManagerBackEnd()
    await new_qrm.init_backend()

    # validate that previous request is still in the correct state:
    assert await new_qrm.is_request_active(new_token_job_1)
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)
    result = await new_qrm.get_resource_req_resp(new_token_job_1)
    assert res_1.name and res_2.name in result.names


@pytest.mark.asyncio
async def test_cancel_move_pending_status(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    result_job_1 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    await qrm_backend_with_db.get_new_token(job1['token'])
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    result_job_2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    # enforce waiting for new request to be active (got new token):
    await qrm_backend_with_db.get_new_token(job2["token"])

    assert await redis_db_object.get_resource_status(res_1) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_2) == ACTIVE_STATUS

    res_1_jobs = await redis_db_object.get_resource_jobs(res_1)
    res_2_jobs = await redis_db_object.get_resource_jobs(res_2)

    # res_1: [job_2_token, job_1_token],  res_2: [job_1_token]
    # cancel the job, res_1 and res_2 should be in pending state since there is a job waiting in queue:
    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_1)

    assert await wait_for_resource_status(redis_db_object, res_1.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_2.name, PENDING_STATUS)

    await cancel_all_open_tasks([result_job_1, result_job_2])


@pytest.mark.asyncio
async def test_new_move_to_pending_state(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    await qrm_backend_with_db.new_request(user_request)
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    await qrm_backend_with_db.cancel_request(new_token_job_1)

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    # since res_1 and res_2 has different token than the requested token, it should be move to PENDING state:
    assert await wait_for_resource_status(redis_db_object, res_1.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_2.name, PENDING_STATUS)

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_new_move_to_pending_state_all_token_resources(redis_db_object, qrm_backend_with_db):
    # new request which destroys token, should cancel the old token and move to pending state:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    res_3 = Resource(name='res3', type='type1', status=ACTIVE_STATUS)
    res_4 = Resource(name='res4', type='type1', status=ACTIVE_STATUS)

    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)

    # add job1 to res_1 ,res_2 and res_3 queues:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name, res_3.name], count=3)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    rrr = await qrm_backend_with_db.get_resource_req_resp(new_token_job_1)

    assert res_1.name and res_2.name and res_3.name in rrr.names

    assert await redis_db_object.get_resource_status(res_1) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_2) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_3) == ACTIVE_STATUS

    # cancel the token and send another request with new token only for one resource:

    await qrm_backend_with_db.cancel_request(new_token_job_1)

    # after cancel, the resource should be in active state:
    assert await redis_db_object.get_resource_status(res_1) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_2) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_3) == ACTIVE_STATUS

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    assert await wait_for_resource_status(redis_db_object, res_1.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_2.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_3.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_4.name, ACTIVE_STATUS)


@pytest.mark.asyncio
async def test_cancel_not_move_to_pending_status(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    # TODO test unstable
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # the new move the resources to pending, we need to move them back to active state:
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    # assert await redis_db_object.get_resource_status(res_1) == ACTIVE_STATUS
    # assert await redis_db_object.get_resource_status(res_2) == ACTIVE_STATUS

    # res_1: [job_1_token],  res_2: [job_1_token]
    # cancel the job, res_1 and res_2 should not be in pending state since there aren't other jobs waiting in queue:
    await qrm_backend_with_db.cancel_request(active_token_job_1)
    assert await redis_db_object.get_resource_status(res_1) == ACTIVE_STATUS
    assert await redis_db_object.get_resource_status(res_2) == ACTIVE_STATUS
    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_wait_for_active_state_on_resource(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add old_token job to res_1:
    await add_token_to_resources(qrm_backend_with_db, [res_1])

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    task = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # since res_1 has different token than the requested token, it should be move to PENDING state:
    assert await wait_for_resource_status(redis_db_object, res_1.name, PENDING_STATUS)

    # request is still active
    await asyncio.sleep(0.1)
    assert await qrm_backend_with_db.is_request_active(new_token_job_1)

    await asyncio.sleep(0.1)
    # move the resources to ACTIVE state, request should be filled:
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    result_job_1 = await qrm_backend_with_db.get_resource_req_resp(new_token_job_1)
    assert res_1.name in result_job_1.names
    assert res_2.name in result_job_1.names

    await cancel_all_open_tasks([task])


@pytest.mark.asyncio
async def test_job_not_added_to_disabled_resource(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=DISABLED_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    result_job_2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    await qrm_backend_with_db.get_new_token(job2['token'])

    assert await redis_db_object.get_active_job(res_1) == {}
    active_job_res_2 = await redis_db_object.get_active_job(res_2)
    assert new_token_job_1 == active_job_res_2['token']

    # after cancel, job2 should get response since it's waiting only in res_2 queue
    await qrm_backend_with_db.cancel_request(new_token_job_1)
    result_job_2 = await result_job_2
    assert res_2.name in result_job_2.names


@pytest.mark.asyncio
async def test_validate_new_request_resources_disabled(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=DISABLED_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)

    # requested both res_1 and res_2 but res_2 is in disabled status, validation should fail:
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(job1["token"])
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert 'not enough available resources' in result.message


@pytest.mark.asyncio
async def test_validate_new_request_not_enough_res_in_db(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, 'res_2'], count=2)

    # requested both res_1 and res_2 but res_2 not in db, validation should fail:
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(job1["token"])
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert 'not enough available resources' in result.message


@pytest.mark.asyncio
async def test_one_res_from_two_another_same_request(redis_db_object, qrm_backend_with_db):
    # two active resources
    # new request: 1 res from 2 -> fill
    # new request: 1 res from 2 -> fill

    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # new request: 1 res from 2 -> fill
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name or res_2.name in result.names
    assert len(result.names) == 1

    # new request: 1 res from 2 -> fill
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name or res_2.name in result.names
    assert len(result.names) == 1


@pytest.mark.asyncio
async def test_basic_req_by_tags(redis_db_object, qrm_backend_with_db):
    # one resource, send request by tag -> fill

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)

    # send req by tag
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name in result.names


@pytest.mark.asyncio
async def test_request_one_tag_for_two_res(redis_db_object, qrm_backend_with_db):
    # two resources with the same tag
    # send request with count 2 by this tag -> filled

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # send req by tag
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=2)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name and res_2.name in result.names


@pytest.mark.asyncio
async def test_req_two_tags(redis_db_object, qrm_backend_with_db):
    # three resources, each resource has unique tag

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, tags=['vlan'])
    res_3 = Resource(name='res3', type='type1', status=ACTIVE_STATUS, tags=['foo'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)

    # send req by tag
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    user_request.add_request_by_tags(['vlan'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name and res_2.name in result.names
    assert res_3.name not in result.names


@pytest.mark.asyncio
async def test_req_existing_token_in_queue_dont_add_to_queue(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name in result.names
    res_1_jobs = await redis_db_object.get_resource_jobs(res_1)
    assert len(res_1_jobs) == 2  # [job1, {}]

    # send request with the same token again:
    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    req_token_only = ResourcesRequest(token=new_token)
    await qrm_backend_with_db.new_request(req_token_only)
    res_1_jobs = await redis_db_object.get_resource_jobs(res_1)
    assert len(res_1_jobs) == 2  # [job1, {}]


@pytest.mark.asyncio
async def test_is_token_active_in_queue(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert res_1.name in result.names
    assert result.is_token_active_in_queue

    # cancel req -> job is no longer active in queue:
    await qrm_backend_with_db.cancel_request(new_token)
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert not result.is_token_active_in_queue

    # new request with same token -> job is active now:
    req_token_only = ResourcesRequest(token=new_token)
    await qrm_backend_with_db.new_request(req_token_only)
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert result.is_token_active_in_queue


@pytest.mark.asyncio
async def test_tags_doesnt_match_any_resources(redis_db_object, qrm_backend_with_db):
    # request by tags -> no matched resources found

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)

    # send req by tag for not existing tag
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['non_exist_tag'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(token=job1['token'])
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert not result.is_valid
    assert 'no matched resources' in result.message


@pytest.mark.asyncio
async def test_new_req_same_req_waiting_get_active_token(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)

    # send new request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token_1 = await qrm_backend_with_db.get_new_token(token=job1['token'])

    # send new request (different token) -> waiting in queue:
    user_request.add_request_by_token(token=job2['token'])
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    # send same request again with the new token:
    user_request.add_request_by_token(new_token_2)
    result = await qrm_backend_with_db.new_request(user_request)
    assert result.token == new_token_2
    assert new_token_2 == await qrm_backend_with_db.get_new_token(new_token_2)


@pytest.mark.asyncio
async def test_new_req_on_cancelled_token(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)

    # send new request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token_1 = await qrm_backend_with_db.get_new_token(token=job1['token'])

    # send new request (different token) -> waiting in queue:
    user_request.add_request_by_token(token=job2['token'])
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    # cancel token_2:
    await qrm_backend_with_db.cancel_request(new_token_2)

    # verify token_2 is no longer valid:
    rrr = await qrm_backend_with_db.get_resource_req_resp(new_token_2)
    assert not rrr.is_valid
    assert rrr.token == new_token_2


@pytest.mark.asyncio
async def test_dont_use_disabled_resource(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    qrm_backend_with_db.use_pending_logic = True

    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # send new request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_tags(['server'], count=2)
    await qrm_backend_with_db.new_request(user_request)
    new_token_1 = await qrm_backend_with_db.get_new_token(token=job1['token'])

    await qrm_backend_with_db.cancel_request(new_token_1)

    # set res_1 to disabled and send new request:
    await redis_db_object.set_resource_status(res_1, DISABLED_STATUS)
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2['token'])
    user_request.add_request_by_tags(['server'], count=1)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_2 = await qrm_backend_with_db.get_new_token(token=job2['token'])

    assert await redis_db_object.get_resource_status(res_1) == DISABLED_STATUS
    assert await redis_db_object.get_resource_status(res_2) == PENDING_STATUS


@pytest.mark.asyncio
async def test_resource_dont_move_pending_after_token_already_destroyed(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    qrm_backend_with_db.use_pending_logic = True

    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server', 'res1'])
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, tags=['server', 'res2'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # send new request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1['token'])
    user_request.add_request_by_tags(['server'], count=2)
    await qrm_backend_with_db.new_request(user_request)
    new_token_1 = await qrm_backend_with_db.get_new_token(token=job1['token'])

    await qrm_backend_with_db.cancel_request(new_token_1)

    # after cancel the old token, send new request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2['token'])
    user_request.add_request_by_tags(['res1'], count=1)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_2 = await qrm_backend_with_db.get_new_token(job2['token'])

    # the new token moved the resources to pending:
    assert await wait_for_resource_status(redis_db_object, res_1.name, PENDING_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_2.name, PENDING_STATUS)

    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    # send again the first request:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(new_token_1)
    user_request.add_request_by_tags(['server'], count=1)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    # the sleep is in to allow the token to be changed (1 sec resolution)
    await asyncio.sleep(1)

    assert await wait_for_resource_status(redis_db_object, res_1.name, ACTIVE_STATUS)
    assert await wait_for_resource_status(redis_db_object, res_2.name, ACTIVE_STATUS)


async def remove_job_and_set_event_after_timeout(timeout_sec: float, token_job_1: str, qrm_be: QueueManagerBackEnd,
                                                 redis, token_job_2: str):
    await asyncio.sleep(timeout_sec)
    await redis.remove_job(token=token_job_2)
    # new_token_job_1 = await redis.get_active_token_from_user_token(token_job_1)
    new_token_job_1 = await qrm_be.get_new_token(token_job_1)
    qrm_be.tokens_change_event[new_token_job_1].set()


async def cancel_all_open_tasks(tasks) -> None:
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def wait_for_resource_status(redis_db_object: RedisDB, resource_name, status, timeout: float = 1):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            logging.error(f'Resource {resource_name} did not reach status {status}')
            return False
        resource = await redis_db_object.get_resource_by_name(resource_name)
        if resource.status == status:
            break
        await asyncio.sleep(0.1)
    return True


async def add_token_to_resources(qrm_backend_with_db, resources: List[Resource]):
    user_request = ResourcesRequest()
    user_request.add_request_by_token('old_token')
    for resource in resources:
        user_request.add_request_by_names([resource.name], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token('old_token')
    await qrm_backend_with_db.cancel_request(new_token)


async def test_managed_token(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest(auto_managed=True)
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name in result.names
    res_1_jobs = await redis_db_object.get_resource_jobs(res_1)
    auto_managed_tokens = await redis_db_object.get_all_auto_managed_tokens()
    assert len(res_1_jobs) == 2  # [job1, {}]
    assert len(auto_managed_tokens) == 1
    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert new_token in auto_managed_tokens
    assert new_token in token_last_update_dict


async def test_managed_token_last_update_time(redis_db_object, qrm_backend_with_db):
    # validate that last_update time is updated after is_request_active called

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest(auto_managed=True)
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()

    old_update_time = token_last_update_dict[new_token]

    await asyncio.sleep(1.01)  # this sleep is bc out time resolution is 1 second
    await qrm_backend_with_db.is_request_active(new_token)
    # after is_request_active called, the update_time must be changed:
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    new_update_time = token_last_update_dict[new_token]
    assert new_update_time != old_update_time

    await asyncio.sleep(1.01)  # this sleep is bc out time resolution is 1 second
    await qrm_backend_with_db.is_request_active(new_token)

    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert new_update_time != token_last_update_dict[new_token]


async def test_cancel_request_remove_token_last_update_and_managed(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest(auto_managed=True)
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)
    assert res_1.name in result.names
    res_1_jobs = await redis_db_object.get_resource_jobs(res_1)
    auto_managed_tokens = await redis_db_object.get_all_auto_managed_tokens()
    assert len(res_1_jobs) == 2  # [job1, {}]
    assert len(auto_managed_tokens) == 1
    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert new_token in auto_managed_tokens
    assert new_token in token_last_update_dict

    # cancel request should remove the token from last_update and from auto_managed:
    await qrm_backend_with_db.cancel_request(new_token)
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    auto_managed_tokens = await redis_db_object.get_all_auto_managed_tokens()
    assert not token_last_update_dict
    assert not auto_managed_tokens


async def test_token_status_cancelled_token(redis_db_object, qrm_backend_with_db):
    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, tags=['server'])
    await redis_db_object.add_resource(res_1)
    user_request = ResourcesRequest(auto_managed=True)
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_tags(['server'], count=1)
    result = await qrm_backend_with_db.new_request(user_request)

    new_token = await qrm_backend_with_db.get_new_token(job1['token'])
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert new_token in token_last_update_dict

    # cancel request should remove the token from last_update
    await qrm_backend_with_db.cancel_request(new_token)
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert not token_last_update_dict

    # send status request on cancelled token and verify that the last_update doesn't exist:
    await qrm_backend_with_db.is_request_active(new_token)
    token_last_update_dict = await redis_db_object.get_all_tokens_last_update()
    assert not token_last_update_dict


async def test_token_multiple_tags_waiting_for_one_tag(redis_db_object, qrm_backend_with_db):
    # this test is to validate that if we have a request with multiple tags, and one of them is not available,
    # the request will be in waiting status until the tag is available but will be in active status for the other tags
    res_1 = Resource(name='res1', type='res_type1', token='old_token1', status=ACTIVE_STATUS, tags=['res_type1'])
    res_2 = Resource(name='res2', type='res_type1', token='old_token2', status=ACTIVE_STATUS, tags=['res_type1'])
    res_3 = Resource(name='res3', type='res_type2', token='old_token1', status=ACTIVE_STATUS, tags=['res_type2'])
    res_4 = Resource(name='res4', type='res_type2', token='old_token2', status=ACTIVE_STATUS, tags=['res_type2'])
    res_5 = Resource(name='res5', type='res_type3', token='old_token2', status=ACTIVE_STATUS, tags=['res_type3'])
    res_6 = Resource(name='res6', type='res_type3', token='old_token2', status=ACTIVE_STATUS, tags=['res_type3'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)
    await redis_db_object.add_resource(res_4)
    await redis_db_object.add_resource(res_5)
    await redis_db_object.add_resource(res_6)

    user_request1 = ResourcesRequest(token='token1')
    user_request1.add_request_by_names(names=['res1'], count=1)
    user_request1.add_request_by_names(names=['res3'], count=1)

    result1 = await qrm_backend_with_db.new_request(user_request1)

    user_request2 = ResourcesRequest(token='token2')
    user_request2.add_request_by_tags(tags=['res_type1'], count=1)
    user_request2.add_request_by_tags(tags=['res_type2'], count=2)
    user_request2.add_request_by_tags(tags=['res_type3'], count=1)

    # res_1 (type1): [token1]
    # res_2 (type1): [token2]
    # res_3 (type2): [token1, token2]
    # res_4 (type2): [token2]
    # res_5 (type3): [token2]
    # res_6 (type3): [token2]

    # token2 should be removed from res_5 and res_6 since it needs only one from them even though that the request
    # is not yet completed since token2 is still waiting for resource from type2 (count=2)

    fut2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request2))

    await asyncio.sleep(0.1)

    new_jobs_res_5 = await redis_db_object.get_resource_jobs(res_5)
    new_jobs_res_6 = await redis_db_object.get_resource_jobs(res_6)

    # validate that the request is now active only in one from the two resources: res5 and res6
    # since it should be active in one of them and be cancelled in the other:
    assert len(new_jobs_res_5) != len(new_jobs_res_6)
    token2 = await qrm_backend_with_db.get_new_token('token2')
    orig_req = await redis_db_object.get_orig_request(token2)
    a = 1


async def test_get_response_by_request_order_after_tags_rearrange(redis_db_object, qrm_backend_with_db):
    # in this test we send new request for multiple resources by tags while the request partially filled for
    # specific tag.  the other tag is blocking the response, until it is released.
    # verify that the response order is according to the request order
    res_1 = Resource(name='res1', type='res_type1', token='old_token1', status=ACTIVE_STATUS, tags=['res_type1'])
    res_2 = Resource(name='res2', type='res_type2', token='old_token1', status=ACTIVE_STATUS, tags=['res_type2'])
    res_3 = Resource(name='res3', type='res_type3', token='old_token2', status=ACTIVE_STATUS, tags=['res_type3'])

    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)

    user_request1 = ResourcesRequest(token='token1')
    user_request1.add_request_by_names(names=['res1'], count=1)
    user_request1.add_request_by_names(names=['res3'], count=1)

    result1 = await qrm_backend_with_db.new_request(user_request1)

    user_request2 = ResourcesRequest(token='token2')
    user_request2.add_request_by_tags(tags=['res_type1'], count=1)
    user_request2.add_request_by_tags(tags=['res_type2'], count=1)
    user_request2.add_request_by_tags(tags=['res_type3'], count=1)

    fut2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request2))

    # res_1 (type1): [token1, token2]
    # res_2 (type2): [token2]
    # res_3 (type3): [token1, token2]

    await asyncio.sleep(0.1)

    new_token_1 = await qrm_backend_with_db.get_new_token('token1')
    await qrm_backend_with_db.cancel_request(new_token_1)

    # request2 should be filled now, check it's order:

    await fut2

    new_token_2 = await qrm_backend_with_db.get_new_token('token2')

    resp_2 = await qrm_backend_with_db.get_resource_req_resp(new_token_2)

    assert ['res1', 'res2', 'res3'] == resp_2.names  # this is the request order and it must be saved

    is_active =  await qrm_backend_with_db.is_request_active(token=new_token_2)

    assert not is_active


async def test_get_response_by_request_backward_compatible_order(redis_db_object, qrm_backend_with_db):
    # this test verifies upgrade of qrm_server after the change in response order, since the ORIG_REQUEST in db
    # is a new field which doesn't exist in older versions of qrm_server
    res_1 = Resource(name='res1', type='res_type1', token='old_token1', status=ACTIVE_STATUS, tags=['res_type1'])
    res_2 = Resource(name='res2', type='res_type2', token='old_token1', status=ACTIVE_STATUS, tags=['res_type2'])
    res_3 = Resource(name='res3', type='res_type3', token='old_token2', status=ACTIVE_STATUS, tags=['res_type3'])

    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.add_resource(res_3)

    user_request1 = ResourcesRequest(token='token1')
    user_request1.add_request_by_names(names=['res1'], count=1)
    user_request1.add_request_by_names(names=['res3'], count=1)

    result1 = await qrm_backend_with_db.new_request(user_request1)

    user_request2 = ResourcesRequest(token='token2')
    user_request2.add_request_by_tags(tags=['res_type1'], count=1)
    user_request2.add_request_by_tags(tags=['res_type2'], count=1)
    user_request2.add_request_by_tags(tags=['res_type3'], count=1)

    fut2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request2))

    # res_1 (type1): [token1, token2]
    # res_2 (type2): [token2]
    # res_3 (type3): [token1, token2]

    await asyncio.sleep(0.1)

    token_1_new = await qrm_backend_with_db.get_new_token('token1')
    token_2_new = await qrm_backend_with_db.get_new_token('token2')

    # verify orig request exists:
    token_2_orig_request = await redis_db_object.get_orig_request(token_2_new)
    assert ResourcesByTags(tags=['res_type1'], count=1) in token_2_orig_request.tags

    # delete token from orig request:
    await redis_db_object.redis.hdel('orig_requests', token_2_new)
    token_2_orig_request = await redis_db_object.get_orig_request(token_2_new)
    assert token_2_orig_request == ResourcesRequest()

    # now orig_request of token2 does not exist, cancel token1 and verify it filled:
    await qrm_backend_with_db.cancel_request(token_1_new)
    await fut2
    resp2 = await qrm_backend_with_db.get_resource_req_resp(token_2_new)
    assert 'res1' and 'res2' and 'res3' in resp2.names
