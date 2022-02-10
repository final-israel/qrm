import time
import pytest
import asyncio
from qrm_server.qrm_http_server import main
from aiohttp import web
from qrm_server.resource_definition import Resource, resource_request_response_to_json
from qrm_server.q_manager import QueueManagerBackEnd, QrmIfc, \
    ResourcesRequest, ResourcesRequestResponse
from qrm_client.qrm_http_client import QrmClient
from db_adapters import redis_adapter
from pytest_redis import factories
from qrm_server import management_server
from qrm_server import qrm_http_server
import json
from concurrent.futures import ProcessPoolExecutor
import aiohttp
from multiprocessing import Process
import qrm_server.qrm_http_server
# noinspection DuplicatedCode


def test_http_server_and_client_get_root_url(full_qrm_servers_ports, redis_db_object):
    ports_dict = full_qrm_servers_ports
    qrm_client_obj = QrmClient(server_ip='127.0.0.1',
                               server_port=ports_dict['http_port'],
                               user_name='test_user')
    qrm_client_obj.wait_for_server_up()
    out = qrm_client_obj.get_root_url()
    assert 'server up 1' == out.text