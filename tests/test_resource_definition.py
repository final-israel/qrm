from qrm_server.resource_definition import ResourcesRequestResponse


def test_resources_request_response_from_json():
    json_str = '{"names": [], "token": "", "request_complete": false, "is_valid": true, "message": ""}'
    rrr_exp = ResourcesRequestResponse()
    rrr = ResourcesRequestResponse.from_json(json_str=json_str)
    assert rrr.as_dict() == rrr_exp.as_dict()
