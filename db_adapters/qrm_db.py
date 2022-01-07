from abc import ABC, abstractmethod
from qrm_server.resource_definition import Resource, ResourcesRequest, ResourcesRequestResponse
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
    async def remove_resource(self, resource: Resource) -> bool:
        pass

    @abstractmethod
    async def set_resource_status(self, resource: Resource, status: str) -> bool:
        pass

    @abstractmethod
    async def get_resource_status(self, resource: Resource) -> str:
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
    async def remove_job(self, job_id: int, resources_list: List[Resource] = None) -> None:
        pass

    @abstractmethod
    async def get_all_resources_dict(self) -> Dict[str, Resource]:
        pass

    @abstractmethod
    async def get_job_for_resource_by_id(self, resource: Resource, job_id: str) -> str:
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
