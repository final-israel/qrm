from qrm_resources.resource_definition import ResourcesRequest, ResourcesByName, ResourceStatus
from qrm_server.qrm_http_server import URL_POST_CANCEL_TOKEN, URL_GET_ROOT, URL_POST_NEW_REQUEST, URL_GET_TOKEN_STATUS,\
    URL_GET_IS_SERVER_UP
from qrm_server.management_server import MGMT_STATUS_API, SET_RESOURCE_STATUS
import logging
import json
import requests
import time


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
    else:
        logging.info(f'send to url {full_url}, params={params}')

    try:
        resp = requests.get(full_url, params=params)
        logging.info(f'full url by requests {resp.url}')
    except Exception as e:
        logging.critical(f'{e}')
        return

    if resp.status_code != 200:
        logging.critical(f'there is an critical error: {str(resp)}')
    return resp


def return_response(res: requests.Response, *args, **kwargs) -> requests.Response:
    # noinspection PyBroadException
    try:
        if res.status_code == 200:
            return res
        else:
            logging.critical(res)
            return res
    except Exception as e:
        logging.critical(e)
        return res


class QrmClient(object):
    def __init__(self, server_ip: str,
                 server_port: str,
                 user_name: str,
                 user_password: str = '',
                 *args,
                 **kwargs):
        self.server_ip: str = server_ip
        self.server_port: str = server_port
        self.user_name: str = user_name
        self.token: str = ''
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

    def _send_cancel(self, token: str, *args, **kwargs) -> requests.Response:
        res_req = ResourcesRequest()
        res_req.token = token
        full_url = self.full_url(URL_POST_CANCEL_TOKEN)
        logging.info(f'send cancel on token = {self.token} to url {full_url}')
        json_as_dict = res_req.as_dict()
        post_to_url(full_url=full_url, data_json=json_as_dict)
        resp = requests.post(full_url, json=json_as_dict)
        return resp

    def send_cancel(self, token: str, *args, **kwargs) -> requests.Response:
        res = self._send_cancel(token)
        return return_response(res)

    def get_root_url(self, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_GET_ROOT)
        logging.info(f'send request to root url {full_url}')
        return get_from_url(full_url=full_url)

    def _new_request(self, data_json: str, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_POST_NEW_REQUEST)
        logging.info(f'send new request with json = {data_json} to url {full_url}')
        resp = post_to_url(full_url=full_url, data_json=data_json)
        return resp

    @staticmethod
    def valid_new_request(resp_data: dict) -> None:
        mandatory_keys =['token', 'is_valid']
        for mand_key in mandatory_keys:
            if resp_data.get(mand_key) is None:
                logging.error(f'the mandatory key {mand_key} is not in the response {resp_data}')

    def new_request(self, data_json: str, *args, **kwargs) -> dict:
        """

        :param data_json:
        :param args:
        :param kwargs:
        :return: read valid
        {'token': str
        is_valid: bool
        more...
        }
        """
        resp = self._new_request(data_json=data_json)
        resp_json = resp.json()
        resp_data = json.loads(resp_json)
        self.valid_new_request(resp_data)
        return resp_data

    def _get_token_status(self, token: str, *args, **kwargs) -> requests.Response:
        full_url = self.full_url(URL_GET_TOKEN_STATUS)
        logging.info(f'send get token status token= {token} to url {full_url}')
        resp = get_from_url(full_url=full_url, params={'token': token})
        return resp

    def get_token_status(self, token: str, *args, **kwargs) -> dict:
        resp = self._get_token_status(token)
        resp_data = resp.json()
        if isinstance(resp_data, str):
            resp_data = json.loads(resp_data)
        return resp_data

    def wait_for_token_ready(self, token: str, timeout: float = float('Inf'), polling_sleep_time: float = 5,
                             *args, **kwargs) -> dict:
        logging.info(f'token ready timeout set to {timeout}')
        resp_data = self.get_token_status(token=token)
        return self.polling_api_status(resp_data, timeout, token, polling_sleep_time=polling_sleep_time)

    def polling_api_status(self, resp_data: dict, timeout: float, token: str, polling_sleep_time: float = 5) -> dict:
        start_time = time.time()
        while not resp_data.get('request_complete'):
            time_d = int(time.time() - start_time)
            logging.info(f'waiting for token {token} to be ready. wait for {time_d} sec, {resp_data}')
            if time_d > timeout:
                logging.warning(f'TIMEOUT! waiting from QRM server has timed out! timeout was set to {timeout}, '
                                f'canceling the token {token}')
                resp = self.send_cancel(token)  # on timeout cancel the token
                resp_data = json.loads(resp.json())
                raise TimeoutError(f'got timeout while waiting for token {token} status complete')
            time.sleep(polling_sleep_time)
            resp_data = self.get_token_status(token=token)
        return resp_data

    def wait_for_server_up(self) -> dict:
        full_url = self.full_url(URL_GET_IS_SERVER_UP)
        logging.info(f'call api is server up {full_url}')
        try_again = True
        while try_again:
            try:
                resp = get_from_url(full_url)
                if resp is not None:
                    break
                time.sleep(0.1)
            except Exception as e:
                logging.error(f'there is a problem! {e}')
                time.sleep(0.1)
        logging.info(f'call api is server up server is: {resp}')
        resp_data = resp.json()
        while not resp_data.get('status'):
            time.sleep(0.1)
        return resp_data


class ManagementClient(object):
    def __init__(self, server_ip: str,
                 server_port: str,
                 user_name: str,
                 user_password: str = '',
                 *args,
                 **kwargs):
        self.server_ip: str = server_ip
        self.server_port: str = server_port
        self.user_name: str = user_name
        self.user_password: str = user_password
        self.init_log_massage()

    def init_log_massage(self) -> None:
        logging.info(f"""init new qrm management client with params:
                mgmt server ip: {self.server_ip}
                mgmt server port: {self.server_port}
                user name: {self.user_name}
                """)

    def get_resource_status(self, resource_name: str) -> str:
        full_url = f'http://{self.server_ip}:{self.server_port}{MGMT_STATUS_API}'
        resp = get_from_url(full_url=full_url)
        status_dict = resp.json()
        res_status = status_dict.get('resources_status').get(resource_name).get('status')
        if not res_status:
            logging.error(f'can\'t find status for resource {resource_name}')
            return ''
        logging.info(f'resource {resource_name} is in status {res_status}')
        return res_status

    def set_resource_status(self, resource_name: str, resource_status: str) -> requests.Response:
        full_url = f'http://{self.server_ip}:{self.server_port}{SET_RESOURCE_STATUS}'
        res_status = ResourceStatus(
            resource_name,
            resource_status
        )
        resp = post_to_url(full_url, data_json=res_status.as_dict())
        return return_response(resp)


if __name__ == '__main__':
    qrm_client = QrmClient(server_ip='127.0.0.1',
                           server_port='5555',
                           user_name='ronsh')

    qrm_client.send_cancel(token='1234_2022_02_03_15_21_42')
    exit(0)
    rr = ResourcesRequest()
    rr.token = '1234'
    rbs = ResourcesByName(names=['a1'], count=1)
    rr.names.append(rbs)
    resp = qrm_client.new_request(rr.as_json())
    print(resp.get('token'))
    result = qrm_client.get_token_status(resp.get('token'))
    print(result)

