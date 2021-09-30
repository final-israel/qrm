import aioredis
import asyncio
import json
import logging


class QueueManager(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.client_name = ''                       # type: str
        self.redis = aioredis.from_url(
            "redis://localhost", encoding="utf-8", decode_responses=True
        )
        self.loop = asyncio.get_running_loop()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        peer_name = transport.get_extra_info('peername')
        logging.info('Connection from {}'.format(peer_name))
        self.transport = transport

    def data_received(self, data) -> None:
        message = data.decode()
        try:
            message_dict = json.loads(message)
            logging.debug('Data received: {!r}'.format(message_dict))
        except json.JSONDecodeError as exc:
            logging.error(f'can\'t convert message to json: {message}\n{exc}')

        logging.info('Send: {!r}'.format(message))
        self.transport.write(data)

    def connection_lost(self, exc: Exception or None) -> None:
        logging.info(f'connection closed: {exc}')
        self.transport.close()


async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: QueueManager(),
        '127.0.0.1', 8888)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
