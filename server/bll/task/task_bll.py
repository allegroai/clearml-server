from collections import OrderedDict
from datetime import datetime
from operator import attrgetter
from random import random
from time import sleep
from typing import Collection, Sequence, Tuple, Any, Optional, List, Dict

import pymongo.results
import six
from mongoengine import Q
from six import string_types

import database.utils as dbutils
import es_factory
from apierrors import errors
from apimodels.tasks import Artifact as ApiArtifact
from config import config
from database.errors import translate_errors_context
from database.model.model import Model
from database.model.project import Project
from database.model.task.metrics import EventStats, MetricEventStats
from database.model.task.output import Output
from database.model.task.task import (
    Task,
    TaskStatus,
    TaskStatusMessage,
    TaskSystemTags,
    ArtifactModes,
    Artifact,
)
from database.utils import get_company_or_none_constraint, id as create_id
from service_repo import APICall
from timing_context import TimingContext
from utilities.dicts import deep_merge
from .utils import ChangeStatusRequest, validate_status_change, ParameterKeyEscaper

log = config.logger(__file__)


class TaskBLL(object):
    def __init__(self, events_es=None):
        self.events_es = (
            events_es if events_es is not None else es_factory.connect("events")
        )

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
        company_id,
        task_id,
        required_status=None,
        required_dataset=None,
        only_fields=None,
    ):

        with TimingContext("mongo", "task_by_id_all"):
            qs = Task.objects(id=task_id, company=company_id)
            if only_fields:
                qs = (
                    qs.only(only_fields)
                    if isinstance(only_fields, string_types)
                    else qs.only(*only_fields)
                )
                qs = qs.only(
                    "status", "input"
                )  # make sure all fields we rely on here are also returned
            task = qs.first()

        if not task:
            raise errors.bad_request.InvalidTaskId(id=task_id)

        if required_status and not task.status == required_status:
            raise errors.bad_request.InvalidTaskStatus(expected=required_status)

        if required_dataset and required_dataset not in (
            entry.dataset for entry in task.input.view.entries
        ):
            raise errors.bad_request.InvalidId(
                "not in input view", dataset=required_dataset
            )

        return task

    @staticmethod
    def assert_exists(company_id, task_ids, only=None, allow_public=False):
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
                res = q.only(*only)
                count = len(res)
            else:
                count = q.count()
                res = q.first()
            if count != len(ids):
                raise errors.bad_request.InvalidTaskId(ids=task_ids)
            return res

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
        company_id,
        user_id,
        task_id,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        parent: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
        system_tags: Optional[Sequence[str]] = None,
        execution_overrides: Optional[dict] = None,
    ) -> Task:
        task = cls.get_by_id(company_id=company_id, task_id=task_id)
        execution_dict = task.execution.to_proper_dict() if task.execution else {}
        if execution_overrides:
            parameters = execution_overrides.get("parameters")
            if parameters is not None:
                execution_overrides["parameters"] = {
                    ParameterKeyEscaper.escape(k): v for k, v in parameters.items()
                }
            execution_dict = deep_merge(execution_dict, execution_overrides)
        artifacts = execution_dict.get("artifacts")
        if artifacts:
            execution_dict["artifacts"] = [
                a for a in artifacts if a.get("mode") != ArtifactModes.output
            ]
        now = datetime.utcnow()

        with translate_errors_context():
            new_task = Task(
                id=create_id(),
                user=user_id,
                company=company_id,
                created=now,
                last_update=now,
                name=name or task.name,
                comment=comment or task.comment,
                parent=parent or task.parent,
                project=project or task.project,
                tags=tags or task.tags,
                system_tags=system_tags or [],
                type=task.type,
                script=task.script,
                output=Output(destination=task.output.destination)
                if task.output
                else None,
                execution=execution_dict,
            )
            cls.validate(new_task)
            new_task.save()

        return new_task

    @classmethod
    def validate(cls, task: Task):
        assert isinstance(task, Task)

        if task.parent and not Task.get(
            company=task.company, id=task.parent, _only=("id",), include_public=True
        ):
            raise errors.bad_request.InvalidTaskId("invalid parent", parent=task.parent)

        if task.project and not Project.get_for_writing(
            company=task.company, id=task.project
        ):
            raise errors.bad_request.InvalidProjectId(id=task.project)

        cls.validate_execution_model(task)

    @staticmethod
    def get_unique_metric_variants(company_id, project_ids=None):
        pipeline = [
            {
                "$match": dict(
                    company=company_id,
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
        task_ids: Collection[str], company_id: str, last_update: datetime
    ):
        return Task.objects(id__in=task_ids, company=company_id).update(
            upsert=False, last_update=last_update
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
                extra_updates[op_path("set", *path)] = value
                if path[-1] == "value":
                    extra_updates[op_path("min", *path[:-1], "min_value")] = value
                    extra_updates[op_path("max", *path[:-1], "max_value")] = value

        if last_events is not None:

            def events_per_type(metric_data: Dict[str, dict]) -> Dict[str, EventStats]:
                return {
                    event_type: EventStats(last_update=event["timestamp"])
                    for event_type, event in metric_data.items()
                }

            metric_stats = {
                dbutils.hash_field_name(metric_key): MetricEventStats(
                    metric=metric_key, event_stats_by_type=events_per_type(metric_data),
                )
                for metric_key, metric_data in last_events.items()
            }
            extra_updates["metric_stats"] = metric_stats

        Task.objects(id=task_id, company=company_id).update(
            upsert=False, last_update=last_update, **extra_updates
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

    @classmethod
    def add_or_update_artifacts(
        cls, task_id: str, company_id: str, artifacts: List[ApiArtifact]
    ) -> Tuple[List[str], List[str]]:
        key = attrgetter("key", "mode")

        if not artifacts:
            return [], []

        with translate_errors_context(), TimingContext("mongo", "update_artifacts"):
            artifacts: List[Artifact] = [
                Artifact(**artifact.to_struct()) for artifact in artifacts
            ]

            attempts = int(config.get("services.tasks.artifacts.update_attempts", 10))

            for retry in range(attempts):
                task = cls.get_task_with_access(
                    task_id, company_id=company_id, requires_write_access=True
                )

                current = list(map(key, task.execution.artifacts))
                updated = [a for a in artifacts if key(a) in current]
                added = [a for a in artifacts if a not in updated]

                filter = {"_id": task_id, "company": company_id}
                update = {}
                array_filters = None
                if current:
                    filter["execution.artifacts"] = {
                        "$size": len(current),
                        "$all": [
                            *(
                                {"$elemMatch": {"key": key, "mode": mode}}
                                for key, mode in current
                            )
                        ],
                    }
                else:
                    filter["$or"] = [
                        {"execution.artifacts": {"$exists": False}},
                        {"execution.artifacts": {"$size": 0}},
                    ]

                if added:
                    update["$push"] = {
                        "execution.artifacts": {"$each": [a.to_mongo() for a in added]}
                    }
                if updated:
                    update["$set"] = {
                        f"execution.artifacts.$[artifact{index}]": artifact.to_mongo()
                        for index, artifact in enumerate(updated)
                    }
                    array_filters = [
                        {
                            f"artifact{index}.key": artifact.key,
                            f"artifact{index}.mode": artifact.mode,
                        }
                        for index, artifact in enumerate(updated)
                    ]

                if not update:
                    return [], []

                result: pymongo.results.UpdateResult = Task._get_collection().update_one(
                    filter=filter,
                    update=update,
                    array_filters=array_filters,
                    upsert=False,
                )

                if result.matched_count >= 1:
                    break

                wait_msec = random() * int(
                    config.get("services.tasks.artifacts.update_retry_msec", 500)
                )

                log.warning(
                    f"Failed to update artifacts for task {task_id} (updated by another party),"
                    f" retrying {retry+1}/{attempts} in {wait_msec}ms"
                )

                sleep(wait_msec / 1000)
            else:
                raise errors.server_error.UpdateFailed(
                    "task artifacts updated by another party"
                )

            return [a.key for a in added], [a.key for a in updated]

    @staticmethod
    def get_aggregated_project_execution_parameters(
        company_id,
        project_ids: Sequence[str] = None,
        page: int = 0,
        page_size: int = 500,
    ) -> Tuple[int, int, Sequence[str]]:

        page = max(0, page)
        page_size = max(1, page_size)

        pipeline = [
            {
                "$match": {
                    "company": company_id,
                    "execution.parameters": {"$exists": True, "$gt": {}},
                    **({"project": {"$in": project_ids}} if project_ids else {}),
                }
            },
            {"$project": {"parameters": {"$objectToArray": "$execution.parameters"}}},
            {"$unwind": "$parameters"},
            {"$group": {"_id": "$parameters.k"}},
            {"$sort": {"_id": 1}},
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
                ParameterKeyEscaper.unescape(r["_id"])
                for r in result.get("results", [])
            ]
            remaining = max(0, total - (len(results) + page * page_size))

        return total, remaining, results
