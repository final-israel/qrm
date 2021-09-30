from abc import ABC, abstractmethod

PREFIX_NAME = 'resource_name'


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