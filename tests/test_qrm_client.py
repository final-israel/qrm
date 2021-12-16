from qrm_server import qrm_client


def test_qrm_resource_request():
    qrmc = qrm_client.QrmClient()
    resources_list = ['res1', 'res2', ('res3', 'res4')]
    response = qrmc.ask_for_resources(resources_list)
    assert 'res1' in response
    assert 'res2' in response
    assert 'res3' in response
