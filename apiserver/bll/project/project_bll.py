from collections import defaultdict
from datetime import datetime
from itertools import groupby
from operator import itemgetter
from typing import Sequence, Optional, Type, Tuple, Dict

from mongoengine import Q, Document

from apiserver import database
from apiserver.apierrors import errors
from apiserver.config_repo import config
from apiserver.database.model import EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskStatus
from apiserver.database.utils import get_options
from apiserver.timing_context import TimingContext
from apiserver.tools import safe_get

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
                {"set__last_change": datetime.utcnow()}
                if hasattr(entity_cls, "last_change")
                else {}
            )
            entity_cls.objects(company=company, id__in=ids).update(
                set__project=project, **extra
            )

            return project

    archived_tasks_cond = {"$in": [EntityVisibility.archived.value, "$system_tags"]}

    @classmethod
    def make_projects_get_all_pipelines(
        cls,
        company_id: str,
        project_ids: Sequence[str],
        specific_state: Optional[EntityVisibility] = None,
    ) -> Tuple[Sequence, Sequence]:
        archived = EntityVisibility.archived.value

        def ensure_valid_fields():
            """
            Make sure system tags is always an array (required by subsequent $in in archived_tasks_cond
            """
            return {
                "$addFields": {
                    "system_tags": {
                        "$cond": {
                            "if": {"$ne": [{"$type": "$system_tags"}, "array"]},
                            "then": [],
                            "else": "$system_tags",
                        }
                    },
                    "status": {"$ifNull": ["$status", "unknown"]},
                }
            }

        status_count_pipeline = [
            # count tasks per project per status
            {
                "$match": {
                    "company": {"$in": [None, "", company_id]},
                    "project": {"$in": project_ids},
                }
            },
            ensure_valid_fields(),
            {
                "$group": {
                    "_id": {
                        "project": "$project",
                        "status": "$status",
                        archived: cls.archived_tasks_cond,
                    },
                    "count": {"$sum": 1},
                }
            },
            # for each project, create a list of (status, count, archived)
            {
                "$group": {
                    "_id": "$_id.project",
                    "counts": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count",
                            archived: "$_id.%s" % archived,
                        }
                    },
                }
            },
        ]

        def runtime_subquery(additional_cond):
            return {
                # the sum of
                "$sum": {
                    # for each task
                    "$cond": {
                        # if completed and started and completed > started
                        "if": {
                            "$and": [
                                "$started",
                                "$completed",
                                {"$gt": ["$completed", "$started"]},
                                additional_cond,
                            ]
                        },
                        # then: floor((completed - started) / 1000)
                        "then": {
                            "$floor": {
                                "$divide": [
                                    {"$subtract": ["$completed", "$started"]},
                                    1000.0,
                                ]
                            }
                        },
                        "else": 0,
                    }
                }
            }

        group_step = {"_id": "$project"}

        for state in EntityVisibility:
            if specific_state and state != specific_state:
                continue
            if state == EntityVisibility.active:
                group_step[state.value] = runtime_subquery(
                    {"$not": cls.archived_tasks_cond}
                )
            elif state == EntityVisibility.archived:
                group_step[state.value] = runtime_subquery(cls.archived_tasks_cond)

        runtime_pipeline = [
            # only count run time for these types of tasks
            {
                "$match": {
                    "company": {"$in": [None, "", company_id]},
                    "type": {"$in": ["training", "testing", "annotation"]},
                    "project": {"$in": project_ids},
                }
            },
            ensure_valid_fields(),
            {
                # for each project
                "$group": group_step
            },
        ]

        return status_count_pipeline, runtime_pipeline

    @classmethod
    def get_project_stats(
        cls,
        company: str,
        project_ids: Sequence[str],
        specific_state: Optional[EntityVisibility] = None,
    ) -> Dict[str, dict]:
        if not project_ids:
            return {}

        status_count_pipeline, runtime_pipeline = cls.make_projects_get_all_pipelines(
            company, project_ids=project_ids, specific_state=specific_state
        )

        default_counts = dict.fromkeys(get_options(TaskStatus), 0)

        def set_default_count(entry):
            return dict(default_counts, **entry)

        status_count = defaultdict(lambda: {})
        key = itemgetter(EntityVisibility.archived.value)
        for result in Task.aggregate(status_count_pipeline):
            for k, group in groupby(sorted(result["counts"], key=key), key):
                section = (
                    EntityVisibility.archived if k else EntityVisibility.active
                ).value
                status_count[result["_id"]][section] = set_default_count(
                    {
                        count_entry["status"]: count_entry["count"]
                        for count_entry in group
                    }
                )

        runtime = {
            result["_id"]: {k: v for k, v in result.items() if k != "_id"}
            for result in Task.aggregate(runtime_pipeline)
        }

        def get_status_counts(project_id, section):
            path = "/".join((project_id, section))
            return {
                "total_runtime": safe_get(runtime, path, 0),
                "status_count": safe_get(status_count, path, default_counts),
            }

        report_for_states = [
            s for s in EntityVisibility if not specific_state or specific_state == s
        ]

        return {
            project: {
                task_state.value: get_status_counts(project, task_state.value)
                for task_state in report_for_states
            }
            for project in project_ids
        }

    @classmethod
    def get_projects_with_active_user(
        cls,
        company: str,
        users: Sequence[str],
        project_ids: Optional[Sequence[str]] = None,
        allow_public: bool = True,
    ) -> Sequence[str]:
        """Get the projects ids where user created any tasks"""
        company = (
            {"company__in": [None, "", company]}
            if allow_public
            else {"company": company}
        )
        projects = {"project__in": project_ids} if project_ids else {}
        return Task.objects(**company, user__in=users, **projects).distinct(
            field="project"
        )
