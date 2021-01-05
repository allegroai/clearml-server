from datetime import datetime
from typing import Sequence, Optional, Type

from mongoengine import Q, Document

from apiserver import database
from apiserver.apierrors import errors
from apiserver.config_repo import config
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
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

    @classmethod
    def create(
        cls,
        user: str,
        company: str,
        name: str,
        description: str,
        tags: Sequence[str] = None,
        system_tags: Sequence[str] = None,
        default_output_destination: str = None,
    ) -> str:
        """
        Create a new project.
        Returns project ID
        """
        now = datetime.utcnow()
        project = Project(
            id=database.utils.id(),
            user=user,
            company=company,
            name=name,
            description=description,
            tags=tags,
            system_tags=system_tags,
            default_output_destination=default_output_destination,
            created=now,
            last_update=now,
        )
        project.save()
        return project.id

    @classmethod
    def find_or_create(
        cls,
        user: str,
        company: str,
        project_name: str,
        description: str,
        project_id: str = None,
        tags: Sequence[str] = None,
        system_tags: Sequence[str] = None,
        default_output_destination: str = None,
    ) -> str:
        """
        Find a project named `project_name` or create a new one.
        Returns project ID
        """
        if not project_id and not project_name:
            raise ValueError("project id or name required")

        if project_id:
            project = Project.objects(company=company, id=project_id).only("id").first()
            if not project:
                raise errors.bad_request.InvalidProjectId(id=project_id)
            return project_id

        project = Project.objects(company=company, name=project_name).only("id").first()
        if project:
            return project.id

        return cls.create(
            user=user,
            company=company,
            name=project_name,
            description=description,
            tags=tags,
            system_tags=system_tags,
            default_output_destination=default_output_destination,
        )

    @classmethod
    def move_under_project(
        cls,
        entity_cls: Type[Document],
        user: str,
        company: str,
        ids: Sequence[str],
        project: str = None,
        project_name: str = None,
    ):
        """
        Move a batch of entities to `project` or a project named `project_name` (create if does not exist)
        """
        with TimingContext("mongo", "move_under_project"):
            project = cls.find_or_create(
                user=user,
                company=company,
                project_id=project,
                project_name=project_name,
                description="Auto-generated during move",
            )
            extra = (
                {"set__last_update": datetime.utcnow()}
                if hasattr(entity_cls, "last_update")
                else {}
            )

            entity_cls.objects(company=company, id__in=ids).update(
                set__project=project, **extra
            )

            return project
