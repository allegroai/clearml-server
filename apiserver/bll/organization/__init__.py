from collections import defaultdict
from enum import Enum
from typing import Sequence, Dict

from apiserver.config_repo import config
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task
from apiserver.redis_manager import redman
from .tags_cache import _TagsCache

log = config.logger(__file__)


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
        company_id: str,
        entity: Tags,
        include_system: bool = False,
        filter_: Dict[str, Sequence[str]] = None,
        projects: Sequence[str] = None,
    ) -> dict:
        tags_cache = self._get_tags_cache_for_entity(entity)
        if not projects:
            return tags_cache.get_tags(
                company_id, include_system=include_system, filter_=filter_
            )

        ret = defaultdict(set)
        for project in projects:
            project_tags = tags_cache.get_tags(
                company_id,
                include_system=include_system,
                filter_=filter_,
                project=project,
            )
            for field, tags in project_tags.items():
                ret[field] |= tags

        return ret

    def update_tags(
        self, company_id: str, entity: Tags, project: str, tags=None, system_tags=None,
    ):
        tags_cache = self._get_tags_cache_for_entity(entity)
        tags_cache.update_tags(company_id, project, tags, system_tags)

    def reset_tags(self, company_id: str, entity: Tags, projects: Sequence[str]):
        tags_cache = self._get_tags_cache_for_entity(entity)
        tags_cache.reset_tags(company_id, projects=projects)

    def _get_tags_cache_for_entity(self, entity: Tags) -> _TagsCache:
        return self._task_tags if entity == Tags.Task else self._model_tags
