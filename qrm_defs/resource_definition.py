import json
import pickle
from dataclasses import dataclass, asdict, field
from typing import List
from datetime import datetime

PENDING_STATUS = 'pending'
ACTIVE_STATUS = 'active'
DISABLED_STATUS = 'disabled'
RESOURCE_NAME_PREFIX = 'resource_name'
ALLOWED_SERVER_STATUSES = [ACTIVE_STATUS, DISABLED_STATUS, PENDING_STATUS]
DATE_FMT = '%Y_%m_%d_%H_%M_%S'
RESOURCES_REQUEST_RESPONSE_VERSION = 1


def resource_from_json(resource_as_json: json):
    return Resource(**json.loads(resource_as_json))


def resource_from_pickle(resource_as_pickle: pickle):
    return Resource(**pickle.loads(resource_as_pickle))


def generate_token_from_seed(seed: str) -> str:
    # replace the datetime of the token if exists, or add datetime in case it's a seed token
    if is_token_format(seed):
        seed_as_list = seed.split('_')[0:-6]
        seed = '_'.join(seed_as_list)
    return f'{seed}_{datetime.now().strftime(DATE_FMT)}'


def is_token_format(token: str) -> bool:
    # token format is: seed_DATE_FMT
    try:
        date_str = '_'.join(token.split('_')[-6:])
        datetime.strptime(date_str, DATE_FMT)
        return True
    except Exception as e:
        return False


def resource_request_from_json(resource_req_as_json: json):  # type:  ResourcesRequest
    res_req = ResourcesRequest()
    res_dict = json.loads(resource_req_as_json)
    res_req.add_request_by_token(res_dict.get('token'))
    for name_req in res_dict['names']:
        res_req.add_request_by_names(**name_req)
    for tags_req in res_dict['tags']:
        res_req.add_request_by_tags(**tags_req)
    return res_req


@dataclass
class Resource:
    name: str
    type: str
    status: str = ''
    token: str = ''
    tags: List[str] = field(default_factory=list)

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
    request_complete: bool = False
    is_valid: bool = True
    message: str = ''
    version: int = RESOURCES_REQUEST_RESPONSE_VERSION
    is_token_active_in_queue: bool = False

    def as_dict(self) -> dict:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_str: str):  # type: ResourcesRequestResponse
        json_as_dict = json.loads(json_str)
        return ResourcesRequestResponse(**json_as_dict)


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


@dataclass
class ResourceStatus:
    resource_name: str = ''
    status: str = ''

    def as_dict(self) -> dict:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict())
