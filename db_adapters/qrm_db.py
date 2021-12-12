from abc import ABC, abstractmethod
from qrm_server.resource_definition import Resource
from typing import List


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
