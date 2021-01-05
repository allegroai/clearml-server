from collections import defaultdict
from enum import Enum
from itertools import chain
from typing import Sequence, Union, Type, Dict

from mongoengine import Q
from redis import Redis

from apiserver.config_repo import config
from apiserver.database.model.base import GetMixin
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task
from apiserver.redis_manager import redman
from apiserver.utilities import json

log = config.logger(__file__)
_settings_prefix = "services.organization"


class _TagsCache:
    _tags_field = "tags"
    _system_tags_field = "system_tags"

    def __init__(self, db_cls: Union[Type[Model], Type[Task]], redis: Redis):
        self.db_cls = db_cls
        self.redis = redis

    @property
    def _tags_cache_expiration_seconds(self):
        return config.get(f"{_settings_prefix}.tags_cache.expiration_seconds", 3600)

    def _get_tags_from_db(
        self,
        company: str,
        field: str,
        project: str = None,
        filter_: Dict[str, Sequence[str]] = None,
    ) -> set:
        query = Q(company=company)
        if filter_:
            for name, vals in filter_.items():
                if vals:
                    query &= GetMixin.get_list_field_query(name, vals)
        if project:
            query &= Q(project=project)

        return self.db_cls.objects(query).distinct(field)

    def _get_tags_cache_key(
        self,
        company: str,
        field: str,
        project: str = None,
        filter_: Dict[str, Sequence[str]] = None,
    ):
        """
        Project None means 'from all company projects'
        The key is built in the way that scanning company keys for 'all company projects'
        will not return the keys related to the particular company projects and vice versa.
        So that we can have a fine grain control on what redis keys to invalidate
        """
        filter_str = None
        if filter_:
            filter_str = "_".join(
                ["filter", *chain.from_iterable([f, *v] for f, v in filter_.items())]
            )
        key_parts = [company, project, self.db_cls.__name__, field, filter_str]
        return "_".join(filter(None, key_parts))

    def get_tags(
        self,
        company: str,
        include_system: bool = False,
        filter_: Dict[str, Sequence[str]] = None,
        project: str = None,
    ) -> dict:
        """
        Get tags and optionally system tags for the company
        Return the dictionary of tags per tags field name
        The function retrieves both cached values from Redis in one call
        and re calculates any of them if missing in Redis
        """
        fields = [self._tags_field]
        if include_system:
            fields.append(self._system_tags_field)
        redis_keys = [
            self._get_tags_cache_key(company, field=f, project=project, filter_=filter_)
            for f in fields
        ]
        cached = self.redis.mget(redis_keys)
        ret = {}
        for field, tag_data, key in zip(fields, cached, redis_keys):
            if tag_data is not None:
                tags = json.loads(tag_data)
            else:
                tags = list(self._get_tags_from_db(company, field, project, filter_))
                self.redis.setex(
                    key,
                    time=self._tags_cache_expiration_seconds,
                    value=json.dumps(tags),
                )
            ret[field] = set(tags)

        return ret

    def update_tags(self, company: str, project: str, tags=None, system_tags=None):
        """
        Updates tags. If reset is set then both tags and system_tags
        are recalculated. Otherwise only those that are not 'None'
        """
        fields = [
            field
            for field, update in (
                (self._tags_field, tags),
                (self._system_tags_field, system_tags),
            )
            if update is not None
        ]
        if not fields:
            return

        self._delete_redis_keys(company, projects=[project], fields=fields)

    def reset_tags(self, company: str, projects: Sequence[str]):
        self._delete_redis_keys(
            company,
            projects=projects,
            fields=(self._tags_field, self._system_tags_field),
        )

    def _delete_redis_keys(
        self, company: str, projects: [Sequence[str]], fields: Sequence[str]
    ):
        redis_keys = list(
            chain.from_iterable(
                self.redis.keys(
                    self._get_tags_cache_key(company, field=f, project=p) + "*"
                )
                for f in fields
                for p in set(projects) | {None}
            )
        )
        if redis_keys:
            self.redis.delete(*redis_keys)


class Tags(Enum):
    Task = "task"
    Model = "model"


class OrgBLL:
    def __init__(self, redis=None):
        self.redis = redis or redman.connection("apiserver")
        self._task_tags = _TagsCache(Task, self.redis)
        self._model_tags = _TagsCache(Model, self.redis)

    def get_tags(
        self,
        company: str,
        entity: Tags,
        include_system: bool = False,
        filter_: Dict[str, Sequence[str]] = None,
        projects: Sequence[str] = None,
    ) -> dict:
        tags_cache = self._get_tags_cache_for_entity(entity)
        if not projects:
            return tags_cache.get_tags(
                company, include_system=include_system, filter_=filter_
            )

        ret = defaultdict(set)
        for project in projects:
            project_tags = tags_cache.get_tags(
                company, include_system=include_system, filter_=filter_, project=project
            )
            for field, tags in project_tags.items():
                ret[field] |= tags

        return ret

    def update_tags(
        self, company: str, entity: Tags, project: str, tags=None, system_tags=None,
    ):
        tags_cache = self._get_tags_cache_for_entity(entity)
        tags_cache.update_tags(company, project, tags, system_tags)

    def reset_tags(self, company: str, entity: Tags, projects: Sequence[str]):
        tags_cache = self._get_tags_cache_for_entity(entity)
        tags_cache.reset_tags(company, projects=projects)

    def _get_tags_cache_for_entity(self, entity: Tags) -> _TagsCache:
        return self._task_tags if entity == Tags.Task else self._model_tags
