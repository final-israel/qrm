import asyncio
import json
import logging
import pytest
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse, ResourcesByName


@pytest.mark.asyncio
async def test_qbackend_find_one_resource(redis_db_object, qrm_backend_with_db):
    exp_resource = Resource(name='res1', type='type1', token='12345')
    await redis_db_object.add_resource(exp_resource)
    await redis_db_object.add_resource(Resource(name='res2', type='type1'))
    await redis_db_object.add_resource(Resource(name='res3', type='type1'))
    exp_resource_list = [exp_resource]
    response = await qrm_backend_with_db.find_resources([exp_resource_list])
    assert len(response) == 1
    assert exp_resource == response[0]


@pytest.mark.asyncio
async def test_qbackend_new_request_by_token_only(redis_db_object, qrm_backend_with_db):
    req_token = '123456'
    res_1 = Resource(name='res1', type='type1', token=req_token)
    res_2 = Resource(name='res2', type='type1', token=req_token)
    res_3 = Resource(name='res3', type='type1', token=req_token)
    res_4 = Resource(name='res4', type='type1', token='1234567')
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
    user_request.add_request_by_token('123456')
    response = await qrm_backend_with_db.new_request(resources_request=user_request)
    # in this case the token is not active in qrm, so the response is only with the requested token:
    assert response == ResourcesRequestResponse(token='123456')


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
    job1 = {'id': token, 'user': 'bar'}
    job2 = {'id': 'other_token', 'user': 'bar'}
    res_1 = Resource(name='res1', type='type1')
    res_2 = Resource(name='res2', type='type1')
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

