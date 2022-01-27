import requests

from qrm_server.resource_definition import ResourcesRequest
from qrm_server.qrm_http_server import URL_POST_CANCEL_TOKEN
#from urllib import request, parse
import logging
import json
import requests
class QrmClient(object):
    def __init__(self, server_ip: str,
                 server_port: str,
                 user_name: str,
                 token: str = '',
                 user_password: str = ''):
        self.server_ip: str = server_ip
        self.server_port: str = server_port
        self.user_name: str = user_name
        self.token: str = token
        self.user_password: str = user_password
        self.init_log_massage()

    def full_url(self, relative_url: str) -> str:
        return f'http://{self.server_ip}:{self.server_port}{relative_url}'

    def init_log_massage(self):
        logging.info(f"""init new qrm client with parms:
                qrm server ip: {self.server_ip}
                qrm server port: {self.server_port}
                user name: {self.user_name}
                token: {self.token}
                """)

    def send_cancel(self) -> requests.Response:
        rr = ResourcesRequest()
        rr.token = self.token
        full_url = self.full_url(URL_POST_CANCEL_TOKEN)
        logging.info(f'send to url {full_url}')
        resp = requests.post(full_url, json=rr.as_dict())
        return resp
