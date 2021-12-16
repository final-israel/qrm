import asyncio
import json
import logging
import time

import pytest
from qrm_server.resource_definition import Resource
from qrm_server.q_manager import QueueManagerBackEnd


@pytest.mark.asyncio
async def test_qbackend_find_a_resources(redis_db_object):
    qrm = QueueManagerBackEnd(redis_port=None)
    exp_resource = Resource(name='res1', type='type1', token='12345')
    await redis_db_object.add_resource(exp_resource)
    await redis_db_object.add_resource(Resource(name='res2', type='type1'))
    await redis_db_object.add_resource(Resource(name='res3', type='type1'))
    qrm.redis = redis_db_object
    response = await qrm.find_resources([exp_resource])
    assert exp_resource == response[0]


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

