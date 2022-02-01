import json
import logging
from aiohttp import web
from http import HTTPStatus
import asyncio
from qrm_server.q_manager import QueueManagerBackEnd
from qrm_server.resource_definition import resource_request_from_json, ResourcesRequestResponse, \
    resource_request_response_to_json

URL_POST_NEW_REQUEST = '/new_request'
URL_GET_TOKEN_STATUS = '/get_token_status'
URL_POST_CANCEL_TOKEN = '/cancel_token'
URL_GET_ROOT = '/'
global qrm_back_end
global global_number
global_number: int = 0


def canceled_token_msg(token):
    return f'canceled token {token}'


def init_qrm_back_end(qrm_back_end_obj: QueueManagerBackEnd) -> None:
    global qrm_back_end
    qrm_back_end = qrm_back_end_obj()


async def new_request(request) -> web.json_response:
    global qrm_back_end  # type: QueueManagerBackEnd
    request_json = await request.json()
    logging.info(f'new request {request_json}')
    resource_request = resource_request_from_json(request_json)
    asyncio.ensure_future(qrm_back_end.new_request(resources_request=resource_request))
    active_token = await qrm_back_end.get_new_token(resource_request.token)
    resp_json = json.dumps({'token': active_token})
    return web.json_response(resp_json, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
async def get_token_status(request) -> web.json_response:
    global qrm_back_end  # type: QueueManagerBackEnd
    logging.info(f'in url get_token_status {request.rel_url}')
    token = request.rel_url.query['token']
    if await qrm_back_end.is_request_active(token=token):
        rrr_obj = ResourcesRequestResponse()
        rrr_obj.request_complete = False
        rrr_json = resource_request_response_to_json(resource_req_res_obj=rrr_obj)
        return web.json_response(rrr_json, status=HTTPStatus.OK)
    else:
        rrr_obj = await qrm_back_end.get_filled_request(token=token)
        rrr_obj.request_complete = True
        rrr_json = resource_request_response_to_json(resource_req_res_obj=rrr_obj)
        return web.json_response(rrr_json, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
async def cancel_token(request) -> web.Response:
    global qrm_back_end  # type: QueueManagerBackEnd
    logging.info('in cancel_token')
    logging.info(request)
    req_dict = await request.json()
    req_dict = json.loads(req_dict)
    token = req_dict.get('token')

    await qrm_back_end.cancel_request(user_token=token)
    return web.Response(status=HTTPStatus.OK,
                        text=f'canceled token {token}')


async def root_url(request) -> web.Response:
    global global_number
    global_number += 1
    return web.Response(status=HTTPStatus.OK,
                        text=f'server up {global_number}')


def main():
    init_qrm_back_end()
    app = web.Application()
    app.add_routes([web.post(f'{URL_POST_NEW_REQUEST}', new_request),
                    web.post(f'{URL_POST_CANCEL_TOKEN}', get_token_status),
                    web.get(f'{URL_GET_TOKEN_STATUS}', cancel_token),
                    web.get(f'/', root_url)])
    web.run_app(app, port=5555)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    try:
        main()
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
