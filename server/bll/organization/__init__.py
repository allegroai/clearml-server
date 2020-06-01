from typing import Sequence

from mongoengine import Q

from config import config
from database.model.base import GetMixin
from database.model.model import Model
from database.model.task.task import Task
from redis_manager import redman
from utilities import json

log = config.logger(__file__)


class OrgBLL:
    _tags_field = "tags"
    _system_tags_field = "system_tags"
    _settings_prefix = "services.organization"

    def __init__(self, redis=None):
        self.redis = redis or redman.connection("apiserver")

    @property
    def _tags_cache_expiration_seconds(self):
        return config.get(
            f"{self._settings_prefix}.tags_cache.expiration_seconds", 3600
        )

    @staticmethod
    def _get_tags_cache_key(company, field: str, filter_: Sequence[str] = None):
        filter_str = "_".join(filter_) if filter_ else ""
        return f"{field}_{company}_{filter_str}"

    @staticmethod
    def _get_tags_from_db(company, field, filter_: Sequence[str] = None) -> set:
        query = Q(company=company)
        if filter_:
            query &= GetMixin.get_list_field_query("system_tags", filter_)

        tags = set()
        for cls_ in (Task, Model):
            tags |= set(cls_.objects(query).distinct(field))
        return tags

    def get_tags(
        self, company, include_system: bool = False, filter_: Sequence[str] = None
    ) -> dict:
        """
        Get tags and optionally system tags for the company
        Return the dictionary of tags per tags field name
        The function retrieves both cached values from Redis in one call
        and re calculates any of them if missing in Redis
        """
        fields = [
            self._tags_field,
            *([self._system_tags_field] if include_system else []),
        ]
        redis_keys = [self._get_tags_cache_key(company, f, filter_) for f in fields]
        cached = self.redis.mget(redis_keys)
        ret = {}
        for field, tag_data, key in zip(fields, cached, redis_keys):
            if tag_data is not None:
                tags = json.loads(tag_data)
            else:
                tags = list(self._get_tags_from_db(company, field, filter_))
                self.redis.setex(
                    key,
                    time=self._tags_cache_expiration_seconds,
                    value=json.dumps(tags),
                )
            ret[field] = tags

        return ret

    def update_org_tags(self, company, tags=None, system_tags=None, reset=False):
        """
        Updates system tags. If reset is set then both tags and system_tags
        are recalculated. Otherwise only those that are not 'None'
        """
        if reset or tags is not None:
            self.redis.delete(self._get_tags_cache_key(company, self._tags_field))
        if reset or system_tags is not None:
            self.redis.delete(
                self._get_tags_cache_key(company, self._system_tags_field)
            )
