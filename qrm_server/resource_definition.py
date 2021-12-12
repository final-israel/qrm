import json
import pickle
from dataclasses import dataclass


RESOURCE_NAME_PREFIX = 'resource_name'
RESOURCE_STATUS_PREFIX = 'resource_status'
ALLOWED_SERVER_STATUSES = ['active', 'disabled']


def load_from_json(resource_as_json: json):
    return Resource(**json.loads(resource_as_json))


def load_from_pickle(resource_as_pickle: pickle):
    return Resource(**pickle.loads(resource_as_pickle))


@dataclass
class Resource:
    name: str
    type: str
    status: str = ''

    def db_name(self) -> str:
        return f'{RESOURCE_NAME_PREFIX}_{self.name}'

    def db_status(self) -> str:
        return f'{RESOURCE_STATUS_PREFIX}_{self.name}'

    def as_dict(self) -> dict:
        return {
            'name': self.name,
            'type': self.type,
            'status': self.status
        }

    def as_json(self) -> str:
        return json.dumps(self.as_dict())

    def as_pickle(self) -> bytes:
        return pickle.dumps(self.as_dict())

    def __eq__(self, other) -> bool:
        return self.name == other.name

    def __str__(self) -> str:
        return f'{self.type}_{self.name}'
