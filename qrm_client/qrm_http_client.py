from qrm_server.resource_definition import ResourcesRequest
from qrm_server.qrm_http_server import URL_POST_CANCEL_TOKEN, URL_GET_ROOT, URL_POST_NEW_REQUEST, URL_GET_TOKEN_STATUS
import logging
import json
import requests


def post_to_url(full_url: str, data_json: dict or str, *args, **kwargs) -> requests.Response or None:
    logging.info(f'post {data_json} to url {full_url}')
    try:
        resp = requests.post(url=full_url, json=data_json)
    except Exception as e:
        logging.critical(f'{e}')
        return

    if resp.status_code != 200:
        logging.critical(f'there is an critical error: {str(resp)}')
    return resp


def get_from_url(full_url: str, params: dict = None, *args, **kwargs) -> requests.Response or None:
    if params is None:
        params = {}
    logging.info(f'send to url {full_url}')
    try:
        resp = requests.get(full_url, params=params)
    except Exception as e:
        logging.critical(f'{e}')
        return

    if resp.status_code != 200:
        logging.critical(f'there is an critical error: {str(resp)}')
    return resp


def return_response(res: requests.Response, *args, **kwargs) -> bool:
    # noinspection PyBroadException
    try:
        if res.status_code == 200:
            return True
        else:
            logging.critical(res)
            return False
    except Exception:
        return False


class QrmClient(object):
    def __init__(self, server_ip: str,
                 server_port: str,
                 user_name: str,
                 token: str = '',
                 user_password: str = '',
                 *args,
                 **kwargs):
        self.server_ip: str = server_ip
        self.server_port: str = server_port
        self.user_name: str = user_name
        self.token: str = token
        self.user_password: str = user_password
        self.init_log_massage()

    def full_url(self, relative_url: str, *args, **kwargs) -> str:
        # noinspection HttpUrlsUsage
        return f'http://{self.server_ip}:{self.server_port}{relative_url}'

    def init_log_massage(self, *args, **kwargs):
        logging.info(f"""init new qrm client with params:
                qrm server ip: {self.server_ip}
                qrm server port: {self.server_port}
                user name: {self.user_name}
                token: {self.token}
                """)

    def _send_cancel(self, *args, **kwargs) -> requests.Response:
        rr = ResourcesRequest()
        rr.token = self.token
        full_url = self.full_url(URL_POST_CANCEL_TOKEN)
        logging.info(f'send cancel ion token = {self.token} to url {full_url}')
        json_as_dict = rr.as_dict()
        post_to_url(full_url=full_url, data_json=json_as_dict)
        resp = requests.post(full_url, json=json_as_dict)
        return resp

    def send_cancel(self, *args, **kwargs) -> bool:
        res = self._send_cancel()
        return return_response(res)

    def get_root_url(self, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_GET_ROOT)
        return get_from_url(full_url=full_url)

    def _new_request(self, data_json: str, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_POST_NEW_REQUEST)
        logging.info(f'send new request with json = {data_json} to url {full_url}')
        resp = post_to_url(full_url=full_url, data_json=data_json)
        return resp

    def new_request(self, data_json: str, *args, **kwargs) -> str:
        resp = self._new_request(data_json=data_json)
        resp_json = resp.json()
        resp_data = json.loads(resp_json)
        return resp_data.get('token')

    def _get_token_status(self, token: str, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_GET_TOKEN_STATUS)
        logging.info(f'send get token status token= {token} to url {full_url}')
        resp = get_from_url(full_url=full_url, params={'token': token})
        return resp

    def get_token_status(self, token: str, *args, **kwargs) -> dict:
        resp = self._get_token_status(token)
        resp_json = resp.json()
        resp_data = json.loads(resp_json)
        return resp_data

    def wait_for_token_ready(self, token: str, *args, **kwargs) -> dict:
        raise NotImplemented


if __name__ == '__main__':
    qrm_client = QrmClient(server_ip='127.0.0.1',
                           server_port='5555',
                           user_name='ronsh',
                           token='1234')
    qrm_client.get_root_url()
