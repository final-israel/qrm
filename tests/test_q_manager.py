import asyncio
import json
import logging
import pytest
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse


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


#@pytest.mark.asyncio
#async def test_qbackend_new_request_by_names_only(redis_db_object, qrm_backend_with_db):
#    user_request = ResourcesRequest()
#    user_request.add_request_by_names(['res1', 'res2', 'res3'])
#    res_1 = Resource(name='res1', type='type1', token='12345')
#    res_2 = Resource(name='res2', type='type1', token='12345')
#    res_3 = Resource(name='res3', type='type1', token='12345')
#    await redis_db_object.add_resource(res_1)
#    await redis_db_object.add_resource(res_2)
#   await redis_db_object.add_resource(res_3)
#    response = await qrm_backend_with_db.new_request(user_request)
#    assert len(response) == 1
#    assert exp_resource == response[0]


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
    user_request = ResourcesRequest()
    user_request.add_request_by_token(req_token)
    user_request.add_request_by_names(names=['res1', 'res2', 'res3'], count=3)
    response = await qrm_backend_with_db.new_request(resources_request=user_request)
    assert req_token == response.token
    assert res_1.name in response.names
    assert res_2.name in response.names
    assert res_3.name in response.names



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

