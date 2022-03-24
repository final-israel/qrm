import argparse
import json
import logging
import datetime
import asyncio
import aiohttp_jinja2
import jinja2
from aiohttp import web
from http import HTTPStatus
from qrm_defs.qrm_urls import URL_POST_NEW_REQUEST, URL_GET_TOKEN_STATUS, URL_POST_CANCEL_TOKEN, URL_GET_ROOT, \
    URL_GET_UPTIME, URL_GET_IS_SERVER_UP
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc
from qrm_defs.resource_definition import resource_request_from_json, ResourcesRequestResponse
from pathlib import Path

LOG_FILE_PATH = '/tmp/log/qrm-server/qrm_server.txt'
HTTP_LISTEN_PORT = 5555
global qrm_back_end
global_number: int = 0

here = Path(__file__).resolve().parent
server_start_time = datetime.datetime.now()


def canceled_token_msg(token):
    return f'canceled token {token}'


async def init_qrm_back_end(qrm_back_end_obj: QrmIfc) -> None:
    global qrm_back_end
    qrm_back_end = qrm_back_end_obj
    await qrm_back_end.init_backend()


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
    rrr_json = rrr_obj.as_json()
    return web.json_response(rrr_json, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
async def get_token_status(request) -> web.json_response:
    global qrm_back_end  # type: QueueManagerBackEnd
    logging.info(f'in url get_token_status {request.rel_url}')
    token = request.rel_url.query['token']
    if await qrm_back_end.is_request_active(token=token):
        rrr_obj = ResourcesRequestResponse()
        rrr_obj.request_complete = False
        rrr_json = rrr_obj.as_json()
        return web.json_response(rrr_json, status=HTTPStatus.OK)
    else:
        rrr_obj = await qrm_back_end.get_resource_req_resp(token=token)
        rrr_obj.request_complete = True
        rrr_json = rrr_obj.as_json()
        return web.json_response(rrr_json, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
async def is_server_up(request) -> web.json_response:
    global qrm_back_end  # type: QueueManagerBackEnd
    logging.info(f'in url is server up {request.rel_url}')
    is_server_up = {'status': True}
    return web.json_response(is_server_up, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
async def cancel_token(request) -> web.Response:
    global qrm_back_end  # type: QueueManagerBackEnd
    logging.info('in cancel_token')
    logging.info(request)
    req_dict = await request.json()
    if isinstance(req_dict, str):
        req_dict = json.loads(req_dict)
    token = req_dict.get('token')
    await qrm_back_end.cancel_request(token=token)
    rrr_obj = ResourcesRequestResponse()
    rrr_obj.request_complete = False
    rrr_obj.token = token
    rrr_obj.message = f'canceled token {rrr_obj.token}'
    rrr_json = rrr_obj.as_json()
    return web.json_response(rrr_json, status=HTTPStatus.OK)


# noinspection PyUnusedLocal
@aiohttp_jinja2.template('base.html')
async def root_url(request) -> web.Response:
    global global_number
    global_number += 1
    return {'global_number': global_number}


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


# noinspection PyUnusedLocal
async def init_qrm_backend(request) -> web.Response:
    logging.info('init qrm backend')
    global qrm_back_end  # type: QueueManagerBackEnd
    await qrm_back_end.init_backend()
    return web.Response(status=HTTPStatus.OK,
                        text=f'init qrm backend')


async def close_qrm_backend(request) -> web.Response:
    logging.info('stop qrm backend')
    global qrm_back_end  # type: QueueManagerBackEnd
    await qrm_back_end.stop_backend()
    return web.Response(status=HTTPStatus.OK,
                        text=f'stop qrm backend')


async def main(use_pending_logic: bool = False):
    # TODO: fix this param:
    await init_qrm_back_end(qrm_back_end_obj=QueueManagerBackEnd(use_pending_logic=use_pending_logic,
                                                                 timeout_for_active_state=2))
    app = web.Application()
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(f'{here}/templates'))
    app.router.add_post(URL_POST_CANCEL_TOKEN, cancel_token)
    app.router.add_post(URL_POST_NEW_REQUEST, new_request)
    app.router.add_get(URL_GET_UPTIME, uptime_url)
    app.router.add_get(URL_GET_ROOT, root_url)
    app.router.add_get(URL_GET_TOKEN_STATUS, get_token_status)
    app.router.add_get(URL_GET_IS_SERVER_UP, is_server_up)
    app.on_startup.append(init_qrm_backend)
    app.on_shutdown.append(close_qrm_backend)
    return app


def config_log(path_to_log_file: str = LOG_FILE_PATH):
    print(f'log file path is: {path_to_log_file}')
    Path(path_to_log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=path_to_log_file, level=logging.DEBUG, format=
    '[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(module)s] [%(message)s]')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)
    logging.info(f'log file path is: {path_to_log_file}')


def run_server(listen_port: int = HTTP_LISTEN_PORT, use_pending_logic: bool = False,
               path_to_log_file: str = LOG_FILE_PATH) -> None:
    config_log(path_to_log_file=path_to_log_file)
    logging.info(f'listening on port {listen_port}')
    logging.info(f'use_pending_logic: {use_pending_logic}')
    web.run_app(main(use_pending_logic), port=listen_port)


def create_parser() -> argparse.ArgumentParser.parse_args:
    parser = argparse.ArgumentParser(description='QRM HTTP SERVER')
    parser.add_argument('--listen_port',
                        help='qrm http server listen port',
                        default=HTTP_LISTEN_PORT)
    parser.add_argument('--use_pending_logic',
                        help='move resource to pending when resource change owners',
                        default=False,
                        action='store_true')

    parser.add_argument('--log_file_path',
                        help='path to text log file',
                        default=LOG_FILE_PATH)

    return parser.parse_args()


if __name__ == "__main__":
    try:
        run_args = create_parser()
        run_server(run_args.listen_port, run_args.use_pending_logic, path_to_log_file=run_args.log_file_path)
    except KeyboardInterrupt as e:
        logging.error(f'got keyboard interrupt: {e}')
