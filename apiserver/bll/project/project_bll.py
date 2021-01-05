from typing import Sequence, Optional

from mongoengine import Q

from apiserver.config import config
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task
from apiserver.timing_context import TimingContext

log = config.logger(__file__)


class ProjectBLL:
    @classmethod
    def get_active_users(
        cls, company, project_ids: Sequence, user_ids: Optional[Sequence] = None
    ) -> set:
        """
        Get the set of user ids that created tasks/models in the given projects
        If project_ids is empty then all projects are examined
        If user_ids are passed then only subset of these users is returned
        """
        with TimingContext("mongo", "active_users_in_projects"):
            res = set()
            query = Q(company=company)
            if project_ids:
                query &= Q(project__in=project_ids)
            if user_ids:
                query &= Q(user__in=user_ids)
            for cls_ in (Task, Model):
                res |= set(cls_.objects(query).distinct(field="user"))

            return res
