from collections import OrderedDict
from datetime import datetime
from typing import Collection, Sequence, Tuple, Any, Optional, Dict

import dpath
import six
from mongoengine import Q
from six import string_types

import apiserver.database.utils as dbutils
from apiserver.apierrors import errors
from apiserver.bll.queue import QueueBLL
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.metrics import EventStats, MetricEventStats
from apiserver.database.model.task.output import Output
from apiserver.database.model.task.task import (
    Task,
    TaskStatus,
    TaskStatusMessage,
    TaskSystemTags,
    ArtifactModes,
    external_task_types,
)
from apiserver.database.model import EntityVisibility
from apiserver.database.utils import get_company_or_none_constraint, id as create_id
from apiserver.es_factory import es_factory
from apiserver.service_repo import APICall
from apiserver.services.utils import validate_tags
from apiserver.timing_context import TimingContext
from apiserver.utilities.parameter_key_escaper import ParameterKeyEscaper
from .artifacts import artifacts_prepare_for_save
from .param_utils import params_prepare_for_save
from .utils import ChangeStatusRequest, validate_status_change, update_project_time

log = config.logger(__file__)
org_bll = OrgBLL()
queue_bll = QueueBLL()
project_bll = ProjectBLL()


class TaskBLL:
    def __init__(self, events_es=None):
        self.events_es = (
            events_es if events_es is not None else es_factory.connect("events")
        )

    @classmethod
    def get_types(cls, company, project_ids: Optional[Sequence]) -> set:
        """
        Return the list of unique task types used by company and public tasks
        If project ids passed then only tasks from these projects are considered
        """
        query = get_company_or_none_constraint(company)
        if project_ids:
            query &= Q(project__in=project_ids)
        res = Task.objects(query).distinct(field="type")
        return set(res).intersection(external_task_types)

    @staticmethod
    def get_task_with_access(
        task_id, company_id, only=None, allow_public=False, requires_write_access=False
    ) -> Task:
        """
        Gets a task that has a required write access
        :except errors.bad_request.InvalidTaskId: if the task is not found
        :except errors.forbidden.NoWritePermission: if write_access was required and the task cannot be modified
        """
        with translate_errors_context():
            query = dict(id=task_id, company=company_id)
            with TimingContext("mongo", "task_with_access"):
                if requires_write_access:
                    task = Task.get_for_writing(_only=only, **query)
                else:
                    task = Task.get(_only=only, **query, include_public=allow_public)

            if not task:
                raise errors.bad_request.InvalidTaskId(**query)

            return task

    @staticmethod
    def get_by_id(
        company_id, task_id, required_status=None, only_fields=None, allow_public=False,
    ):
        if only_fields:
            if isinstance(only_fields, string_types):
                only_fields = [only_fields]
            else:
                only_fields = list(only_fields)
            only_fields = only_fields + ["status"]

        with TimingContext("mongo", "task_by_id_all"):
            tasks = Task.get_many(
                company=company_id,
                query=Q(id=task_id),
                allow_public=allow_public,
                override_projection=only_fields,
                return_dicts=False,
            )
            task = None if not tasks else tasks[0]

        if not task:
            raise errors.bad_request.InvalidTaskId(id=task_id)

        if required_status and not task.status == required_status:
            raise errors.bad_request.InvalidTaskStatus(expected=required_status)

        return task

    @staticmethod
    def assert_exists(
        company_id, task_ids, only=None, allow_public=False, return_tasks=True
    ) -> Optional[Sequence[Task]]:
        task_ids = [task_ids] if isinstance(task_ids, six.string_types) else task_ids
        with translate_errors_context(), TimingContext("mongo", "task_exists"):
            ids = set(task_ids)
            q = Task.get_many(
                company=company_id,
                query=Q(id__in=ids),
                allow_public=allow_public,
                return_dicts=False,
            )
            if only:
                # Make sure to reset fields filters (some fields are excluded by default) since this
                # is an internal call and specific fields were requested.
                q = q.all_fields().only(*only)

            if q.count() != len(ids):
                raise errors.bad_request.InvalidTaskId(ids=task_ids)

            if return_tasks:
                return list(q)

    @staticmethod
    def create(call: APICall, fields: dict):
        identity = call.identity
        now = datetime.utcnow()
        return Task(
            id=create_id(),
            user=identity.user,
            company=identity.company,
            created=now,
            last_update=now,
            last_change=now,
            **fields,
        )

    @staticmethod
    def validate_execution_model(task, allow_only_public=False):
        if not task.execution or not task.execution.model:
            return

        company = None if allow_only_public else task.company
        model_id = task.execution.model
        model = Model.objects(
            Q(id=model_id) & get_company_or_none_constraint(company)
        ).first()
        if not model:
            raise errors.bad_request.InvalidModelId(model=model_id)

        return model

    @classmethod
    def clone_task(
        cls,
        company_id: str,
        user_id: str,
        task_id: str,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        parent: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
        system_tags: Optional[Sequence[str]] = None,
        hyperparams: Optional[dict] = None,
        configuration: Optional[dict] = None,
        execution_overrides: Optional[dict] = None,
        validate_references: bool = False,
        new_project_name: str = None,
    ) -> Tuple[Task, dict]:
        validate_tags(tags, system_tags)
        params_dict = {
            field: value
            for field, value in (
                ("hyperparams", hyperparams),
                ("configuration", configuration),
            )
            if value is not None
        }

        task = cls.get_by_id(company_id=company_id, task_id=task_id, allow_public=True)

        execution_dict = task.execution.to_proper_dict() if task.execution else {}
        execution_model_overriden = False
        if execution_overrides:
            execution_model_overriden = execution_overrides.get("model") is not None
            artifacts_prepare_for_save({"execution": execution_overrides})

            params_dict["execution"] = {}
            for legacy_param in ("parameters", "configuration"):
                legacy_value = execution_overrides.pop(legacy_param, None)
                if legacy_value is not None:
                    params_dict["execution"] = legacy_value

            execution_dict.update(execution_overrides)

        params_prepare_for_save(params_dict, previous_task=task)

        artifacts = execution_dict.get("artifacts")
        if artifacts:
            execution_dict["artifacts"] = {
                k: a
                for k, a in artifacts.items()
                if a.get("mode") != ArtifactModes.output
            }
        execution_dict.pop("queue", None)

        new_project_data = None
        if not project and new_project_name:
            # Use a project with the provided name, or create a new project
            project = ProjectBLL.find_or_create(
                project_name=new_project_name,
                user=user_id,
                company=company_id,
                description="Auto-generated while cloning",
            )
            new_project_data = {"id": project, "name": new_project_name}

        now = datetime.utcnow()

        def clean_system_tags(input_tags: Sequence[str]) -> Sequence[str]:
            if not input_tags:
                return input_tags

            return [
                tag
                for tag in input_tags
                if tag not in [TaskSystemTags.development, EntityVisibility.archived.value]
            ]

        with TimingContext("mongo", "clone task"):
            new_task = Task(
                id=create_id(),
                user=user_id,
                company=company_id,
                created=now,
                last_update=now,
                last_change=now,
                name=name or task.name,
                comment=comment or task.comment,
                parent=parent or task.parent,
                project=project or task.project,
                tags=tags or task.tags,
                system_tags=system_tags or clean_system_tags(task.system_tags),
                type=task.type,
                script=task.script,
                output=Output(destination=task.output.destination)
                if task.output
                else None,
                execution=execution_dict,
                configuration=params_dict.get("configuration") or task.configuration,
                hyperparams=params_dict.get("hyperparams") or task.hyperparams,
            )
            cls.validate(
                new_task,
                validate_model=validate_references or execution_model_overriden,
                validate_parent=validate_references or parent,
                validate_project=validate_references or project,
            )
            new_task.save()

            if task.project == new_task.project:
                updated_tags = tags
                updated_system_tags = system_tags
            else:
                updated_tags = new_task.tags
                updated_system_tags = new_task.system_tags
            org_bll.update_tags(
                company_id,
                Tags.Task,
                project=new_task.project,
                tags=updated_tags,
                system_tags=updated_system_tags,
            )
            update_project_time(new_task.project)

        return new_task, new_project_data

    @classmethod
    def validate(
        cls,
        task: Task,
        validate_model=True,
        validate_parent=True,
        validate_project=True,
    ):
        """
        Validate task properties according to the flag
        Task project is always checked for being writable
        in order to disable the modification of public projects
        """
        if (
            validate_parent
            and task.parent
            and not Task.get(
                company=task.company, id=task.parent, _only=("id",), include_public=True
            )
        ):
            raise errors.bad_request.InvalidTaskId("invalid parent", parent=task.parent)

        if task.project:
            project = Project.get_for_writing(company=task.company, id=task.project)
            if validate_project and not project:
                raise errors.bad_request.InvalidProjectId(id=task.project)

        if validate_model:
            cls.validate_execution_model(task)

    @staticmethod
    def get_unique_metric_variants(company_id, project_ids=None):
        pipeline = [
            {
                "$match": dict(
                    company={"$in": [None, "", company_id]},
                    **({"project": {"$in": project_ids}} if project_ids else {}),
                )
            },
            {"$project": {"metrics": {"$objectToArray": "$last_metrics"}}},
            {"$unwind": "$metrics"},
            {
                "$project": {
                    "metric": "$metrics.k",
                    "variants": {"$objectToArray": "$metrics.v"},
                }
            },
            {"$unwind": "$variants"},
            {
                "$group": {
                    "_id": {
                        "metric": "$variants.v.metric",
                        "variant": "$variants.v.variant",
                    },
                    "metrics": {
                        "$addToSet": {
                            "metric": "$variants.v.metric",
                            "metric_hash": "$metric",
                            "variant": "$variants.v.variant",
                            "variant_hash": "$variants.k",
                        }
                    },
                }
            },
            {"$sort": OrderedDict({"_id.metric": 1, "_id.variant": 1})},
        ]

        with translate_errors_context():
            result = Task.aggregate(pipeline)
            return [r["metrics"][0] for r in result]

    @staticmethod
    def set_last_update(
        task_ids: Collection[str],
        company_id: str,
        last_update: datetime,
        **extra_updates,
    ):
        tasks = Task.objects(id__in=task_ids, company=company_id).only(
            "status", "started"
        )
        for task in tasks:
            updates = extra_updates
            if task.status == TaskStatus.in_progress and task.started:
                updates = {
                    "active_duration": (
                        datetime.utcnow() - task.started
                    ).total_seconds(),
                    **extra_updates,
                }
            Task.objects(id=task.id, company=company_id).update(
                upsert=False,
                last_update=last_update,
                last_change=last_update,
                **updates,
            )

    @staticmethod
    def update_statistics(
        task_id: str,
        company_id: str,
        last_update: datetime = None,
        last_iteration: int = None,
        last_iteration_max: int = None,
        last_scalar_values: Sequence[Tuple[Tuple[str, ...], Any]] = None,
        last_events: Dict[str, Dict[str, dict]] = None,
        **extra_updates,
    ):
        """
        Update task statistics
        :param task_id: Task's ID.
        :param company_id: Task's company ID.
        :param last_update: Last update time. If not provided, defaults to datetime.utcnow().
        :param last_iteration: Last reported iteration. Use this to set a value regardless of current
            task's last iteration value.
        :param last_iteration_max: Last reported iteration. Use this to conditionally set a value only
            if the current task's last iteration value is smaller than the provided value.
        :param last_scalar_values: Last reported metrics summary for scalar events (value, metric, variant).
        :param last_events: Last reported metrics summary (value, metric, event type).
        :param extra_updates: Extra task updates to include in this update call.
        :return:
        """
        last_update = last_update or datetime.utcnow()

        if last_iteration is not None:
            extra_updates.update(last_iteration=last_iteration)
        elif last_iteration_max is not None:
            extra_updates.update(max__last_iteration=last_iteration_max)

        if last_scalar_values is not None:

            def op_path(op, *path):
                return "__".join((op, "last_metrics") + path)

            for path, value in last_scalar_values:
                if path[-1] == "min_value":
                    extra_updates[op_path("min", *path[:-1], "min_value")] = value
                elif path[-1] == "max_value":
                    extra_updates[op_path("max", *path[:-1], "max_value")] = value
                else:
                    extra_updates[op_path("set", *path)] = value

        if last_events is not None:

            def events_per_type(metric_data: Dict[str, dict]) -> Dict[str, EventStats]:
                return {
                    event_type: EventStats(last_update=event["timestamp"])
                    for event_type, event in metric_data.items()
                }

            metric_stats = {
                dbutils.hash_field_name(metric_key): MetricEventStats(
                    metric=metric_key, event_stats_by_type=events_per_type(metric_data)
                )
                for metric_key, metric_data in last_events.items()
            }
            extra_updates["metric_stats"] = metric_stats

        TaskBLL.set_last_update(
            task_ids=[task_id],
            company_id=company_id,
            last_update=last_update,
            **extra_updates,
        )

    @classmethod
    def model_set_ready(
        cls,
        model_id: str,
        company_id: str,
        publish_task: bool,
        force_publish_task: bool = False,
    ) -> tuple:
        with translate_errors_context():
            query = dict(id=model_id, company=company_id)
            model = Model.objects(**query).first()
            if not model:
                raise errors.bad_request.InvalidModelId(**query)
            elif model.ready:
                raise errors.bad_request.ModelIsReady(**query)

            published_task_data = {}
            if model.task and publish_task:
                task = (
                    Task.objects(id=model.task, company=company_id)
                    .only("id", "status")
                    .first()
                )
                if task and task.status != TaskStatus.published:
                    published_task_data["data"] = cls.publish_task(
                        task_id=model.task,
                        company_id=company_id,
                        publish_model=False,
                        force=force_publish_task,
                    )
                    published_task_data["id"] = model.task

            updated = model.update(upsert=False, ready=True)
            return updated, published_task_data

    @classmethod
    def publish_task(
        cls,
        task_id: str,
        company_id: str,
        publish_model: bool,
        force: bool,
        status_reason: str = "",
        status_message: str = "",
    ) -> dict:
        task = cls.get_task_with_access(
            task_id, company_id=company_id, requires_write_access=True
        )
        if not force:
            validate_status_change(task.status, TaskStatus.published)

        previous_task_status = task.status
        output = task.output or Output()
        publish_failed = False

        try:
            # set state to publishing
            task.status = TaskStatus.publishing
            task.save()

            # publish task models
            if task.output.model and publish_model:
                output_model = (
                    Model.objects(id=task.output.model)
                    .only("id", "task", "ready")
                    .first()
                )
                if output_model and not output_model.ready:
                    cls.model_set_ready(
                        model_id=task.output.model,
                        company_id=company_id,
                        publish_task=False,
                    )

            # set task status to published, and update (or set) it's new output (view and models)
            return ChangeStatusRequest(
                task=task,
                new_status=TaskStatus.published,
                force=force,
                status_reason=status_reason,
                status_message=status_message,
            ).execute(published=datetime.utcnow(), output=output)

        except Exception as ex:
            publish_failed = True
            raise ex
        finally:
            if publish_failed:
                task.status = previous_task_status
                task.save()

    @classmethod
    def stop_task(
        cls,
        task_id: str,
        company_id: str,
        user_name: str,
        status_reason: str,
        force: bool,
    ) -> dict:
        """
        Stop a running task. Requires task status 'in_progress' and
        execution_progress 'running', or force=True. Development task or
        task that has no associated worker is stopped immediately.
        For a non-development task with worker only the status message
        is set to 'stopping' to allow the worker to stop the task and report by itself
        :return: updated task fields
        """

        task = cls.get_task_with_access(
            task_id,
            company_id=company_id,
            only=(
                "status",
                "project",
                "tags",
                "system_tags",
                "last_worker",
                "last_update",
            ),
            requires_write_access=True,
        )

        def is_run_by_worker(t: Task) -> bool:
            """Checks if there is an active worker running the task"""
            update_timeout = config.get("apiserver.workers.task_update_timeout", 600)
            return (
                t.last_worker
                and t.last_update
                and (datetime.utcnow() - t.last_update).total_seconds() < update_timeout
            )

        if TaskSystemTags.development in task.system_tags or not is_run_by_worker(task):
            new_status = TaskStatus.stopped
            status_message = f"Stopped by {user_name}"
        else:
            new_status = task.status
            status_message = TaskStatusMessage.stopping

        return ChangeStatusRequest(
            task=task,
            new_status=new_status,
            status_reason=status_reason,
            status_message=status_message,
            force=force,
        ).execute()

    @staticmethod
    def get_aggregated_project_parameters(
        company_id,
        project_ids: Sequence[str] = None,
        page: int = 0,
        page_size: int = 500,
    ) -> Tuple[int, int, Sequence[dict]]:

        page = max(0, page)
        page_size = max(1, page_size)
        pipeline = [
            {
                "$match": {
                    "company": {"$in": [None, "", company_id]},
                    "hyperparams": {"$exists": True, "$gt": {}},
                    **({"project": {"$in": project_ids}} if project_ids else {}),
                }
            },
            {"$project": {"sections": {"$objectToArray": "$hyperparams"}}},
            {"$unwind": "$sections"},
            {
                "$project": {
                    "section": "$sections.k",
                    "names": {"$objectToArray": "$sections.v"},
                }
            },
            {"$unwind": "$names"},
            {"$group": {"_id": {"section": "$section", "name": "$names.k"}}},
            {"$sort": OrderedDict({"_id.section": 1, "_id.name": 1})},
            {
                "$group": {
                    "_id": 1,
                    "total": {"$sum": 1},
                    "results": {"$push": "$$ROOT"},
                }
            },
            {
                "$project": {
                    "total": 1,
                    "results": {"$slice": ["$results", page * page_size, page_size]},
                }
            },
        ]

        with translate_errors_context():
            result = next(Task.aggregate(pipeline), None)

        total = 0
        remaining = 0
        results = []

        if result:
            total = int(result.get("total", -1))
            results = [
                {
                    "section": ParameterKeyEscaper.unescape(
                        dpath.get(r, "_id/section")
                    ),
                    "name": ParameterKeyEscaper.unescape(dpath.get(r, "_id/name")),
                }
                for r in result.get("results", [])
            ]
            remaining = max(0, total - (len(results) + page * page_size))

        return total, remaining, results

    @classmethod
    def dequeue_and_change_status(
        cls, task: Task, company_id: str, status_message: str, status_reason: str,
    ):
        cls.dequeue(task, company_id)

        return ChangeStatusRequest(
            task=task,
            new_status=TaskStatus.created,
            status_reason=status_reason,
            status_message=status_message,
        ).execute(unset__execution__queue=1)

    @classmethod
    def dequeue(cls, task: Task, company_id: str, silent_fail=False):
        """
        Dequeue the task from the queue
        :param task: task to dequeue
        :param company_id: task's company ID.
        :param silent_fail: do not throw exceptions. APIError is still thrown
        :raise errors.bad_request.InvalidTaskId: if the task's status is not queued
        :raise errors.bad_request.MissingRequiredFields: if the task is not queued
        :raise APIError or errors.server_error.TransactionError: if internal call to queues.remove_task fails
        :return: the result of queues.remove_task call. None in case of silent failure
        """
        if task.status not in (TaskStatus.queued,):
            if silent_fail:
                return
            raise errors.bad_request.InvalidTaskId(
                status=task.status, expected=TaskStatus.queued
            )

        if not task.execution or not task.execution.queue:
            if silent_fail:
                return
            raise errors.bad_request.MissingRequiredFields(
                "task has no queue value", field="execution.queue"
            )

        return {
            "removed": queue_bll.remove_task(
                company_id=company_id, queue_id=task.execution.queue, task_id=task.id
            )
        }
