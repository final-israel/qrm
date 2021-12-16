import asyncio
import json
import logging
import pytest


async def test_tcp_echo_client(q_manager_for_test, unused_tcp_port):
    print(f'##### ron test {unused_tcp_port}')
    message = {'server_name': 'test_server'}
    reader, writer = await asyncio.open_connection(
        host='127.0.0.1',
        port=unused_tcp_port
    )

    while True:
        logging.info(f'Send: {message!r}')
        writer.write(json.dumps(message).encode())

        data = await reader.read(100)
        logging.info(f'Received: {data.decode()!r}')

        await asyncio.sleep(5)
    assert True

if __name__ == "__main__":
   pass
    #logging.basicConfig(level=logging.DEBUG)
    #asyncio.run(tcp_echo_client({'server_name': 'test_server'}))

