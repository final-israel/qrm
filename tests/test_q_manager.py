import asyncio
import json
import logging
import pytest

from qrm_defs.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName, \
    PENDING_STATUS, ACTIVE_STATUS, DISABLED_STATUS
from qrm_server.q_manager import QueueManagerBackEnd


@pytest.mark.asyncio
async def test_qbackend_new_request_by_token_only(redis_db_object, qrm_backend_with_db):
    req_token = '123456'
    res_1 = Resource(name='res1', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_3 = Resource(name='res3', type='type1', token=req_token, status=ACTIVE_STATUS)
    res_4 = Resource(name='res4', type='type1', token='1234567', status=ACTIVE_STATUS)
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


@pytest.mark.asyncio
async def test_request_by_token_not_valid(redis_db_object, qrm_backend_with_db):
    req_token = 'other_token'
    res_1 = Resource(name='res1', type='type1', token=req_token)
    res_2 = Resource(name='res2', type='type1', token=req_token)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)
    await redis_db_object.generate_token(req_token, [res_1, res_2])
    user_request = ResourcesRequest()
    req_token = '123456'
    user_request.add_request_by_token(req_token)
    await qrm_backend_with_db.new_request(resources_request=user_request)
    new_token = await qrm_backend_with_db.get_new_token(req_token)
    response = await qrm_backend_with_db.get_resource_req_resp(new_token)

    assert new_token in response.token
    assert 'contains names and tags' in response.message


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
    
    asyncio.ensure_future(remove_job_and_set_event_after_timeout(0.1, token, qrm_backend_with_db, redis_db_object,
                                                                 'other_token'))
    # resources queues: {res_1: [job1, job2], res_2: [job1]}, so currently job2 is active in res_1
    await qrm_backend_with_db.new_request(user_request)
    new_token = await qrm_backend_with_db.get_new_token(token)
    result = await qrm_backend_with_db.get_resource_req_resp(new_token)
    assert res_1.name in result.names
    assert res_2.name in result.names
    assert len(result.names) == 2


@pytest.mark.asyncio
async def test_cancel_request(redis_db_object, qrm_backend_with_db):
    async def _cancel_request(timeout_sec: float, token: str):
        await asyncio.sleep(timeout_sec)
        await qrm_backend_with_db.cancel_request(token)

    def cancel_cb(result):
        try:
            ret = result.result()
        except Exception as exc:
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
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    # at this point the new request is running in BG and still active

    new_active_token = await qrm_backend_with_db.get_new_token(token)
    assert await qrm_backend_with_db.is_request_active(new_active_token)
    await qrm_backend_with_db.cancel_request(new_active_token)
    assert not await qrm_backend_with_db.is_request_active(new_active_token)


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
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    active_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_2)
    while await qrm_backend_with_db.is_request_active(active_token_job_1):
        await asyncio.sleep(0.05)
    result = await qrm_backend_with_db.get_resource_req_resp(active_token_job_1)
    assert res_1.name in result.names
    assert res_2.name in result.names
    assert len(result.names) == 2


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
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))

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
    assert qrm_backend_with_db.is_request_active(active_token_job_3)

    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_1)

    # now after both job1 and job 2 were cancelled, job 3 should be filled:
    res_job_3 = await res_job_3
    assert res_1.name and res_2.name in res_job_3.names


@pytest.mark.asyncio
async def test_recovery_jobs_in_queue(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    job2 = {'token': 'job_2_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS)
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS)
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # remove the old QrmBackend and init a new instance:
    del qrm_backend_with_db
    new_qrm = QueueManagerBackEnd()

    # validate that previous request is still in the correct state:
    assert new_qrm.is_request_active(new_token_job_1)
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
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name], count=1)
    result_job_2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    # enforce waiting for new request to be active (got new token):
    job_2_token = await qrm_backend_with_db.get_new_token(job2["token"])

    # res_1: [job_2_token, job_1_token],  res_2: [job_1_token]
    # cancel the job, res_1 and res_2 should be in pending state since there is a job waiting in queue:
    active_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])
    await qrm_backend_with_db.cancel_request(active_token_job_1)
    assert await redis_db_object.get_resource_status(res_1) == PENDING_STATUS
    assert await redis_db_object.get_resource_status(res_2) == PENDING_STATUS


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
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
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


@pytest.mark.asyncio
async def test_new_move_to_pending_state(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, token='old_token')
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, token=job1['token'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    await qrm_backend_with_db.get_new_token(job1['token'])

    # since res_1 has different token than the requested token, it should be move to PENDING state:
    assert await redis_db_object.get_resource_status(res_1) == PENDING_STATUS


@pytest.mark.asyncio
async def test_wait_for_active_state_on_resource(redis_db_object, qrm_backend_with_db):
    # use the pending logic:
    qrm_backend_with_db.use_pending_logic = True

    job1 = {'token': 'job_1_token'}
    res_1 = Resource(name='res1', type='type1', status=ACTIVE_STATUS, token='old_token')
    res_2 = Resource(name='res2', type='type1', status=ACTIVE_STATUS, token=job1['token'])
    await redis_db_object.add_resource(res_1)
    await redis_db_object.add_resource(res_2)

    # add job1 to res_1 queue and res_2 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job1["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=2)
    asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # since res_1 has different token than the requested token, it should be move to PENDING state:
    assert await redis_db_object.get_resource_status(res_1) == PENDING_STATUS

    # request is still active
    await asyncio.sleep(0.1)
    assert qrm_backend_with_db.is_request_active(new_token_job_1)

    await asyncio.sleep(0.1)
    # move the resources to ACTIVE state, request should be filled:
    await redis_db_object.set_resource_status(res_1, ACTIVE_STATUS)
    await redis_db_object.set_resource_status(res_2, ACTIVE_STATUS)

    result_job_1 = await qrm_backend_with_db.get_resource_req_resp(new_token_job_1)
    assert res_1.name in result_job_1.names
    assert res_2.name in result_job_1.names


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
    result_job_1 = await qrm_backend_with_db.new_request(user_request)
    new_token_job_1 = await qrm_backend_with_db.get_new_token(job1['token'])

    # add job2 to res_1 queue:
    user_request = ResourcesRequest()
    user_request.add_request_by_token(job2["token"])
    user_request.add_request_by_names([res_1.name, res_2.name], count=1)
    result_job_2 = asyncio.ensure_future(qrm_backend_with_db.new_request(user_request))
    new_token_job_2 = await qrm_backend_with_db.get_new_token(job2['token'])

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


async def remove_job_and_set_event_after_timeout(timeout_sec: float, token_job_1: str, qrm_be: QueueManagerBackEnd,
                                                 redis, token_job_2: str):
    await asyncio.sleep(timeout_sec)
    await redis.remove_job(token=token_job_2)
    # new_token_job_1 = await redis.get_active_token_from_user_token(token_job_1)
    new_token_job_1 = await qrm_be.get_new_token(token_job_1)
    qrm_be.tokens_change_event[new_token_job_1].set()


async def tcp_echo_client(message: dict):
    reader, writer = await asyncio.open_connection(
        host='127.0.0.1',
        port=8888
    )

    while True:
        logging.info(f'Send: {message!r}')
        writer.write(json.dumps(message).encode())

        data = await reader.read(100)
        logging.info(f'Received: {data.decode()!r}')

        await asyncio.sleep(5)

    # print('Close the connection')
    # writer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(tcp_echo_client({'server_name': 'test_server'}))

