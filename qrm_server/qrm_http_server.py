import logging
from aiohttp import web
from http import HTTPStatus
import asyncio

# from qrm_server.resource_definition import Resource, resource_from_json

URL_POST_NEW_REQUEST = '/new_request'
URL_GET_TOKEN_STATUS = '/get_token_status'
URL_POST_CANCEL_TOKEN = '/cancel_token'


async def new_request(request):
    return web.Response(status=HTTPStatus.OK,
                        text=f'your token is: foobar {str(request)}\n')


# noinspection PyUnusedLocal
async def get_token_status(request):
    raise NotImplemented


# noinspection PyUnusedLocal
async def cancel_token(request):
    raise NotImplemented


def main():
    app = web.Application()
    app.add_routes([web.post(f'{URL_POST_NEW_REQUEST}', new_request),
                    web.post(f'{URL_POST_CANCEL_TOKEN}', get_token_status),
                    web.get(f'{URL_GET_TOKEN_STATUS}', cancel_token)])
    web.run_app(app)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
