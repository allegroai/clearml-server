from datetime import datetime
from typing import Callable, Tuple, Sequence, Dict, Optional

from mongoengine import Q

from apiserver.apierrors import errors
from apiserver.apimodels.models import ModelTaskPublishResponse
from apiserver.bll.task.utils import deleted_prefix, get_last_metric_updates
from apiserver.config_repo import config
from apiserver.database.model import EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task, TaskStatus
from .metadata import Metadata


class ModelBLL:
    event_bll = None

    @classmethod
    def get_company_model_by_id(
        cls, company_id: str, model_id: str, only_fields=None
    ) -> Model:
        query = dict(company=company_id, id=model_id)
        qs = Model.objects(**query)
        if only_fields:
            qs = qs.only(*only_fields)
        model = qs.first()
        if not model:
            raise errors.bad_request.InvalidModelId(**query)
        return model

    @staticmethod
    def assert_exists(
        company_id, model_ids, only=None, allow_public=False, return_models=True,
    ) -> Optional[Sequence[Model]]:
        model_ids = [model_ids] if isinstance(model_ids, str) else model_ids
        ids = set(model_ids)
        query = Q(id__in=ids)

        q = Model.get_many(
            company=company_id,
            query=query,
            allow_public=allow_public,
            return_dicts=False,
        )
        if only:
            q = q.only(*only)

        if q.count() != len(ids):
            raise errors.bad_request.InvalidModelId(ids=model_ids)

        if return_models:
            return list(q)

    @classmethod
    def publish_model(
        cls,
        model_id: str,
        company_id: str,
        user_id: str,
        force_publish_task: bool = False,
        publish_task_func: Callable[[str, str, str, bool], dict] = None,
    ) -> Tuple[int, ModelTaskPublishResponse]:
        model = cls.get_company_model_by_id(company_id=company_id, model_id=model_id)
        if model.ready:
            raise errors.bad_request.ModelIsReady(company=company_id, model=model_id)

        published_task = None
        if model.task and publish_task_func:
            task = (
                Task.objects(id=model.task, company=company_id)
                .only("id", "status")
                .first()
            )
            if task and task.status != TaskStatus.published:
                task_publish_res = publish_task_func(
                    model.task, company_id, user_id, force_publish_task
                )
                published_task = ModelTaskPublishResponse(
                    id=model.task, data=task_publish_res
                )

        updated = model.update(upsert=False, ready=True, last_update=datetime.utcnow())
        return updated, published_task

    @classmethod
    def delete_model(
        cls, model_id: str, company_id: str, user_id: str, force: bool, delete_external_artifacts: bool = True,
    ) -> Tuple[int, Model]:
        model = cls.get_company_model_by_id(
            company_id=company_id,
            model_id=model_id,
            only_fields=("id", "task", "project", "uri"),
        )
        deleted_model_id = f"{deleted_prefix}{model_id}"

        using_tasks = Task.objects(models__input__model=model_id).only("id")
        if using_tasks:
            if not force:
                raise errors.bad_request.ModelInUse(
                    "as execution model, use force=True to delete",
                    num_tasks=len(using_tasks),
                )
            # update deleted model id in using tasks
            Task._get_collection().update_many(
                filter={"_id": {"$in": [t.id for t in using_tasks]}},
                update={"$set": {"models.input.$[elem].model": deleted_model_id}},
                array_filters=[{"elem.model": model_id}],
                upsert=False,
            )

        if model.task:
            task = Task.objects(id=model.task).first()
            if task:
                now = datetime.utcnow()
                if task.status == TaskStatus.published:
                    if not force:
                        raise errors.bad_request.ModelCreatingTaskExists(
                            "and published, use force=True to delete", task=model.task
                        )
                    Task._get_collection().update_one(
                        filter={"_id": model.task, "models.output.model": model_id},
                        update={
                            "$set": {
                                "models.output.$[elem].model": deleted_model_id,
                                "output.error": f"model deleted on {now.isoformat()}",
                                "last_change": now,
                            },
                        },
                        array_filters=[{"elem.model": model_id}],
                        upsert=False,
                    )
                else:
                    task.update(
                        pull__models__output__model=model_id, set__last_change=now
                    )

        delete_external_artifacts = delete_external_artifacts and config.get(
            "services.async_urls_delete.enabled", True
        )
        if delete_external_artifacts:
            from apiserver.bll.task.task_cleanup import (
                collect_debug_image_urls,
                collect_plot_image_urls,
                _schedule_for_delete,
            )
            urls = set()
            urls.update(collect_debug_image_urls(company_id, model_id))
            urls.update(collect_plot_image_urls(company_id, model_id))
            if model.uri:
                urls.add(model.uri)
            if urls:
                _schedule_for_delete(
                    task_id=model_id,
                    company=company_id,
                    user=user_id,
                    urls=urls,
                    can_delete_folders=False,
                )

        if not cls.event_bll:
            from apiserver.bll.event import EventBLL
            cls.event_bll = EventBLL()

        cls.event_bll.delete_task_events(company_id, model_id, allow_locked=True, model=True)
        del_count = Model.objects(id=model_id, company=company_id).delete()
        return del_count, model

    @classmethod
    def archive_model(cls, model_id: str, company_id: str):
        cls.get_company_model_by_id(
            company_id=company_id, model_id=model_id, only_fields=("id",)
        )
        archived = Model.objects(company=company_id, id=model_id).update(
            add_to_set__system_tags=EntityVisibility.archived.value,
            last_update=datetime.utcnow(),
        )

        return archived

    @classmethod
    def unarchive_model(cls, model_id: str, company_id: str):
        cls.get_company_model_by_id(
            company_id=company_id, model_id=model_id, only_fields=("id",)
        )
        unarchived = Model.objects(company=company_id, id=model_id).update(
            pull__system_tags=EntityVisibility.archived.value,
            last_update=datetime.utcnow(),
        )

        return unarchived

    @classmethod
    def get_model_stats(
        cls, company: str, model_ids: Sequence[str],
    ) -> Dict[str, dict]:
        if not model_ids:
            return {}

        result = Model.aggregate(
            [
                {
                    "$match": {
                        "company": {"$in": [None, "", company]},
                        "_id": {"$in": model_ids},
                    }
                },
                {
                    "$addFields": {
                        "labels_count": {"$size": {"$objectToArray": "$labels"}}
                    }
                },
                {"$project": {"labels_count": 1}},
            ]
        )
        return {r.pop("_id"): r for r in result}

    @staticmethod
    def update_statistics(
        company_id: str,
        model_id: str,
        last_iteration_max: int = None,
        last_scalar_events: Dict[str, Dict[str, dict]] = None,
    ):
        updates = {"last_update": datetime.utcnow()}
        if last_iteration_max is not None:
            updates.update(max__last_iteration=last_iteration_max)

        raw_updates = {}
        if last_scalar_events is not None:
            raw_updates = {}
            if last_scalar_events is not None:
                get_last_metric_updates(
                    task_id=model_id,
                    last_scalar_events=last_scalar_events,
                    raw_updates=raw_updates,
                    extra_updates=updates,
                    model_events=True,
                )

        ret = Model.objects(id=model_id).update_one(**updates)
        if ret and raw_updates:
            Model.objects(id=model_id).update_one(__raw__=[{"$set": raw_updates}])

        return ret
