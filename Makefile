NAME=qrm_server
DIST_DIR=${PWD}/dist
VERSION=debug

.PHONY: build check-local dist major _major minor _minor patch _patch debug
get_ver:
	$(eval VERSION :=$(shell vmn show ${NAME}))
	vmn show --verbose ${NAME} > ./qrm_server/ver.yaml
	@echo "cat ./qrm_server/ver.yaml"
	cat ./qrm_server/ver.yaml

build: check
	@echo "Buliding"
	docker build --network host -f DockerfileQrmServer -t finalil/${NAME}:${VERSION} .
	git checkout -- ./qrm_server/ver.yaml
	docker tag finalil/${NAME}:${VERSION} finalil/${NAME}:latest

major: _major get_ver build _publish
_major:
	@echo "Major Release"
	vmn stamp -r major ${NAME}

minor: _minor get_ver build _publish
_minor:
	@echo "Minor Release"
	vmn stamp -r minor ${NAME}

patch: _patch get_ver build _publish
_patch:
	@echo "Patch Release"
	vmn stamp -r patch ${NAME}

debug: _debug build _publish
_debug:
	@echo "Debug"

_publish:
	docker login
	docker push finalil/${NAME}:${VERSION}
	docker push finalil/${NAME}:latest

check: check-local
check-local:
	@echo "---------------------------------------------------------------"
	@echo "---------------------------------------------------------------"
	@echo "---~               Running unit tests                      ~---"
               
