from qrm_helper_services.auto_managed_tokens_service.auto_managed_tokens_service_main import AutoManagedToken


def test_auto_managed_tokens_get_status(mgmt_client_mock):
    auto_managed_token = AutoManagedToken(qrm_client=None, management_client=mgmt_client_mock)
    status_dict = auto_managed_token._get_status_api_from_server()
    print(status_dict)
    ron=1
