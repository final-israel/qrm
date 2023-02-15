from qrm_client.qrm_http_client import QrmClient, ManagementClient
from qrm_defs.resource_definition import ResourcesByTags, ResourcesRequest, ResourcesRequestResponse


def send_new(qrm_client_obj: QrmClient):
    token = 'my_test_token'
    rr = ResourcesRequest(token=token, tags=[ResourcesByTags(tags=['server'], count=1)])
    out = qrm_client_obj.new_request(data_json=rr.to_json())
    print(out)


def get_token_status(qrm_client_obj: QrmClient):
    token = 'my_test_token_2022_06_01_13_15_34'
    out = qrm_client_obj.get_token_status(token=token)
    print(out)


def cancel_job(qrm_client_obj: QrmClient):
    list_of_tokens = [
        {'token': 'my_test_token_2022_06_01_13_15_34'},
    ]
    for item in list_of_tokens:
        print(f'send cancel on {item}')
        out = qrm_client_obj.send_cancel(item['token'])
        print(out)
        print(out.text)


if __name__ == "__main__":
    qrm_cli = QrmClient(server_ip='127.0.0.1',
                        server_port=5555,
                        user_name='test_user')
    #send_new(qrm_client_obj = qrm_cli)
    #get_token_status(qrm_client_obj=qrm_cli)
    cancel_job(qrm_client_obj=qrm_cli)