import logging
import logging.handlers

from qrm_common.qrm_logger import init_logger
from qrm_client.qrm_client_lib.qrm_http_client import QrmClientIfc, QrmClient, ManagementClient, ManagementClientIfc


class AutoManagedToken(object):
    def __init__(self, qrm_client: QrmClientIfc, management_client: ManagementClientIfc):
        self.qrm_client: QrmClientIfc = qrm_client
        self.management_client: ManagementClientIfc = management_client

    def _get_status_api_from_server(self) -> dict:
        return self.management_client.get_status_api()


def run_service():

    pass
if __name__ == '__main__':
    run_service()