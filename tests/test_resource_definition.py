from qrm_defs.resource_definition import ResourcesRequestResponse


def test_resources_request_response_from_json():
    json_str = '{"names": [], "token": "", "request_complete": false, "is_valid": true, "message": ""}'
    rrr_exp = ResourcesRequestResponse()
    rrr = ResourcesRequestResponse.from_json(json_str)
    assert rrr.to_dict() == rrr_exp.to_dict()
