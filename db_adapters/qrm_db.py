from abc import ABC, abstractmethod

RESOURCE_NAME_PREFIX = 'resource_name'
RESOURCE_STATUS_PREFIX = 'resource_status'
ALLOWED_SERVER_STATUSES = ['active', 'disabled']


def get_resource_name_in_db(resource_name: str) -> str:
    return f'{RESOURCE_NAME_PREFIX}_{resource_name}'


def get_resource_status_in_db(resource_name: str) -> str:
    return f'{RESOURCE_STATUS_PREFIX}_{resource_name}'


class QrmBaseDB(ABC):
    @abstractmethod
    async def get_all_keys_by_pattern(self, pattern: str = None) -> list:
        pass

    @abstractmethod
    async def get_all_resources(self) -> list:
        pass

    @abstractmethod
    async def add_resource(self, resource_name: str) -> None:
        pass

    @abstractmethod
    async def remove_resource(self, resource_name: str) -> bool:
        pass

    @abstractmethod
    async def set_resource_status(self, resource_name: str, status: str) -> bool:
        pass

    @abstractmethod
    async def get_resource_status(self, resource_name: str) -> str:
        pass

    @abstractmethod
    async def add_job_to_resource(self, resource_name: str, job: dict) -> bool:
        pass

    @abstractmethod
    async def get_resource_jobs(self, resource_name: str) -> list:
        pass

    @abstractmethod
    async def set_qrm_status(self, status: str) -> bool:
        pass

    @abstractmethod
    async def get_qrm_status(self) -> None:
        pass
