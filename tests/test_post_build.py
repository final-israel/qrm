import pytest
import docker


def test_post_build_dokcer_image(request):
    if not request.config.option.post_build:
        pytest.skip("--post-build not specified")
    client = docker.from_env()
    'docker pull finalil/qrm_server:latest'
    #client.containers.run(image='finalil/qrm_server:latest', command='--version', detach=False)
    client.containers.run(image='finalil/qrm_server:0.0.18', command=['--help'], detach=False)