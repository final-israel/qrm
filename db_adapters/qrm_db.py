from abc import ABC, abstractmethod
from qrm_defs.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse
from typing import List, Dict


class QrmBaseDB(ABC):
    @abstractmethod
    async def get_all_keys_by_pattern(self, pattern: str = None) -> list:
        pass

    @abstractmethod
    async def get_all_resources(self) -> list:
        pass

    @abstractmethod
    async def add_resource(self, resource: Resource) -> None:
        pass

    @abstractmethod
    async def get_resource_by_name(self, resource_name: str) -> Resource or None:
        pass

    @abstractmethod
    async def get_resources_by_names(self, resources_names: List[str]) -> List[Resource]:
        pass

    @abstractmethod
    async def remove_resource(self, resource: Resource) -> bool:
        pass

    @abstractmethod
    async def set_resource_status(self, resource: Resource, status: str) -> bool:
        pass

    @abstractmethod
    async def get_resource_status(self, resource: Resource) -> str:
        pass

    @abstractmethod
    async def get_resource_type(self, resource: Resource) -> str:
        pass

    @abstractmethod
    async def add_job_to_resource(self, resource: Resource, job: dict) -> bool:
        pass

    @abstractmethod
    async def get_resource_jobs(self, resource: Resource) -> list:
        pass

    @abstractmethod
    async def set_qrm_status(self, status: str) -> bool:
        pass

    @abstractmethod
    async def get_qrm_status(self) -> str:
        pass

    @abstractmethod
    async def is_resource_exists(self, resource: Resource) -> bool:
        pass

    @abstractmethod
    async def remove_job(self, token: int, resources_list: List[Resource] = None) -> None:
        pass

    @abstractmethod
    async def get_all_resources_dict(self) -> Dict[str, Resource]:
        pass

    @abstractmethod
    async def get_job_for_resource_by_id(self, resource: Resource, token: str) -> str:
        pass

    @abstractmethod
    async def generate_token(self, token: str, resources: List[Resource]) -> bool:
        pass

    @abstractmethod
    async def get_token_resources(self, token: str) -> List[Resource]:
        pass

    @abstractmethod
    async def add_resources_request(self, resources_req: ResourcesRequest) -> None:
        pass

    @abstractmethod
    async def get_open_requests(self) -> Dict[str, ResourcesRequest]:
        pass

    @abstractmethod
    async def get_open_request_by_token(self, token: str) -> ResourcesRequest:
        pass

    @abstractmethod
    async def update_open_request(self, token: str, updated_request: ResourcesRequest) -> bool:
        pass

    @abstractmethod
    async def remove_open_request(self, token: str) -> None:
        pass

    @abstractmethod
    async def partial_fill_request(self, token: str, resource: Resource) -> None:
        pass

    @abstractmethod
    async def get_partial_fill(self, token: str) -> ResourcesRequestResponse:
        pass

    @abstractmethod
    async def remove_partially_fill_request(self, token: str) -> None:
        pass

    @abstractmethod
    async def get_active_job(self, resource: Resource) -> dict:
        pass

    @abstractmethod
    async def set_token_for_resource(self, token: str, resource: Resource) -> None:
        pass

    @abstractmethod
    async def is_request_filled(self, token: str) -> bool:
        pass

    @abstractmethod
    async def get_active_token_from_user_token(self, user_token: str) -> str:
        pass

    @abstractmethod
    async def set_active_token_for_user_token(self, user_token: str, active_token: str) -> bool:
        pass

    @abstractmethod
    async def wait_for_resource_active_status(self, resource: Resource) -> None:
        pass

    @abstractmethod
    async def get_req_resp_for_token(self, token: str) -> ResourcesRequestResponse:
        pass

    @abstractmethod
    async def set_req_resp(self, rrr: ResourcesRequestResponse) -> None:
        pass

    @abstractmethod
    async def get_all_open_tokens(self) -> List[str]:
        pass

    @abstractmethod
    async def get_resources_names_by_tags(self, tags: List[str]) -> List[str]:
        pass
