from __future__ import annotations
import json
import pickle
from dataclasses import dataclass, asdict, field
from typing import List
from dataclass_type_validator import dataclass_validate


RESOURCE_NAME_PREFIX = 'resource_name'
ALLOWED_SERVER_STATUSES = ['active', 'disabled']


def resource_from_json(resource_as_json: json):
    return Resource(**json.loads(resource_as_json))


def resource_from_pickle(resource_as_pickle: pickle):
    return Resource(**pickle.loads(resource_as_pickle))


def resource_request_from_json(resource_req_as_json: json):
    return ResourcesRequest(**json.loads(resource_req_as_json))


@dataclass_validate
@dataclass
class Resource:
    name: str
    type: str
    status: str = ''
    token: str = ''

    def db_name(self) -> str:
        return f'{RESOURCE_NAME_PREFIX}_{self.name}'

    def as_dict(self) -> dict:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict())

    def as_pickle(self) -> bytes:
        return pickle.dumps(self.as_dict())

    def __eq__(self, other) -> bool:
        if not isinstance(other, Resource):
            return False
        return self.name == other.name

    def __str__(self) -> str:
        return f'{self.type}_{self.name}'


@dataclass
class ResourcesByName:
    names: List[str]
    count: int


@dataclass
class ResourcesByTags:
    tags: List[str]
    count: int


@dataclass
class ResourcesRequestResponse:
    names: List[str] = field(default_factory=list)
    token: str = ''

@dataclass_validate
@dataclass
class ResourcesRequest:
    names: List[ResourcesByName] = field(default_factory=list)
    tags: List[ResourcesByTags] = field(default_factory=list)
    token: str = ''

    def validate(self) -> None:
        self.validate_not_empty()

    def validate_not_empty(self) -> None:
        assert self.names or self.tags or self.token, 'you must specify at least one from: names, tags or token'

    def add_request_by_tags(self, tags: List[str], count: int) -> None:
        """
        :param tags: the relevant tags that attached to the requested resources
        :param count: number of requested resources from tags list
        :return: None
        """
        self.tags.append(ResourcesByTags(tags, count))

    def add_request_by_names(self, names: List[str], count: int) -> None:
        """
        :param names: list of requested resources names, the qrm will allocate "count" from the resources list,
        count <= len(names)
        :param count: number of requested resources from names list
        :return:
        """
        assert count <= len(names), 'count must be <= number of resources in the list'
        self.names.append(ResourcesByName(names, count))

    def add_request_by_token(self, token: str) -> None:
        self.token = token

    def as_dict(self) -> dict:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict())
