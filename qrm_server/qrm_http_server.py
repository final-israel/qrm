import json
import logging
from aiohttp import web
from http import HTTPStatus
import asyncio
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc
from qrm_server.resource_definition import resource_request_from_json, ResourcesRequestResponse, \
    resource_request_response_to_json
import datetime

URL_POST_NEW_REQUEST = '/new_request'
URL_GET_TOKEN_STATUS = '/get_token_status'
URL_POST_CANCEL_TOKEN = '/cancel_token'
URL_GET_ROOT = '/'
URL_GET_UPTIME = '/uptime'
global qrm_back_end
global_number: int = 0

server_start_time = datetime.datetime.now()


def canceled_token_msg(token):
    return f'canceled token {token}'


def init_qrm_back_end(qrm_back_end_obj: QrmIfc) -> None:
    global qrm_back_end
    qrm_back_end = qrm_back_end_obj


async def new_request(request) -> web.json_response:
    global qrm_back_end  # type: QueueManagerBackEnd
    request_json = await request.json()
    logging.info(f'new request {request_json}')
    resource_request = resource_request_from_json(request_json)
    asyncio.ensure_future(qrm_back_end.new_request(resources_request=resource_request))
    active_token = await qrm_back_end.get_new_token(resource_request.token)
    rrr_obj = ResourcesRequestResponse()
    rrr_obj.request_complete = False
    rrr_obj.token = active_token
    rrr_json = resource_request_response_to_json(resource_req_res_obj=rrr_obj)
    return web.json_response(rrr_json, status=HTTPStatus.OK)


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


# noinspection PyUnusedLocal
async def root_url(request) -> web.Response:
    global global_number
    global_number += 1
    return web.Response(status=HTTPStatus.OK,
                        text=f'server up {global_number}')


# noinspection PyUnusedLocal
async def uptime_url(request) -> web.Response:
    logging.info('url ask for server uptime')

    # noinspection SpellCheckingInspection
    def strfdelta(tdelta, fmt):
        d = {"days": tdelta.days}
        d["hours"], rem = divmod(tdelta.seconds, 3600)
        d["minutes"], d["seconds"] = divmod(rem, 60)
        return fmt.format(**d)

    time_diff_obj = datetime.datetime.now() - server_start_time
    time_str = strfdelta(time_diff_obj, "http server up for {days} days {hours}:{minutes}:{seconds}")
    server_start_str = server_start_time.strftime("%m/%d/%Y, %H:%M:%S")
    full_str = f'server started at {server_start_str}\n{time_str}'
    logging.info(f'url server uptime {full_str}')
    return web.Response(status=HTTPStatus.OK,
                        text=f'server up {full_str}')


def main():
    init_qrm_back_end(qrm_back_end_obj=QueueManagerBackEnd())
    app = web.Application()
    app.router.add_post(URL_POST_CANCEL_TOKEN, cancel_token)
    app.router.add_post(URL_POST_NEW_REQUEST, new_request)
    app.router.add_get(URL_GET_UPTIME, uptime_url)
    app.router.add_get(URL_GET_ROOT, root_url)
    app.router.add_get(URL_GET_TOKEN_STATUS, get_token_status)
    web.run_app(app, port=5555)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    try:
        main()
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
