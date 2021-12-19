import asyncio
import json
import logging
import time

import pytest
from qrm_server.resource_definition import Resource


@pytest.mark.asyncio
async def test_qbackend_find_one_resource(redis_db_object, qrm_backend_with_db):
    exp_resource = Resource(name='res1', type='type1', token='12345')
    await redis_db_object.add_resource(exp_resource)
    await redis_db_object.add_resource(Resource(name='res2', type='type1'))
    await redis_db_object.add_resource(Resource(name='res3', type='type1'))
    response = await qrm_backend_with_db.find_resources([exp_resource])
    assert exp_resource == response[0]


@pytest.mark.asyncio
async def test_qbackend_find_two_resources(redis_db_object, qrm_backend_with_db, resource_foo, resource_bar):
    resource_foo.token = '123'
    resource_bar.token = '123'
    await redis_db_object.add_resource(resource_foo)
    await redis_db_object.add_resource(resource_bar)
    response = await qrm_backend_with_db.find_resources([resource_foo, resource_bar])
    assert len(response) == 2


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

