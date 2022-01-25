import json
import logging
from aiohttp import web
from http import HTTPStatus
import asyncio
from qrm_server.q_manager import QueueManagerBackEnd
# from qrm_server.resource_definition import Resource, resource_from_json

URL_POST_NEW_REQUEST = '/new_request'
URL_GET_TOKEN_STATUS = '/get_token_status'
URL_POST_CANCEL_TOKEN = '/cancel_token'
global qrm_back_end


def init_qrm_back_end(qrm_back_end_obj: QueueManagerBackEnd):
    global qrm_back_end
    qrm_back_end = qrm_back_end_obj()


async def new_request(request):
    global qrm_back_end  # type: QueueManagerBackEnd
    qrm_back_end.new_request()
    return web.Response(status=HTTPStatus.OK,
                        text=f'your token is: foobar {str(request)}\n')


# noinspection PyUnusedLocal
async def get_token_status(request):
    raise NotImplemented


# noinspection PyUnusedLocal
async def cancel_token(request):
    global qrm_back_end  # type: QueueManagerBackEnd
    req_dict = await request.json()
    req_dict = json.loads(req_dict)
    token = req_dict.get('token')
    await qrm_back_end.cancel_request(user_token=token)
    return web.Response(status=HTTPStatus.OK,
                        text=f'canceled token {token}')


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
