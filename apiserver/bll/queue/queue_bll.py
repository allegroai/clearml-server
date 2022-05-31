from collections import defaultdict
from datetime import datetime
from typing import Callable, Sequence, Optional, Tuple

from elasticsearch import Elasticsearch

from apiserver import database
from apiserver.es_factory import es_factory
from apiserver.apierrors import errors
from apiserver.bll.queue.queue_metrics import QueueMetrics
from apiserver.bll.workers import WorkerBLL
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.queue import Queue, Entry

log = config.logger(__file__)


class QueueBLL(object):
    def __init__(self, worker_bll: WorkerBLL = None, es: Elasticsearch = None):
        self.worker_bll = worker_bll or WorkerBLL()
        self.es = es or es_factory.connect("workers")
        self._metrics = QueueMetrics(self.es)

    @property
    def metrics(self) -> QueueMetrics:
        return self._metrics

    @staticmethod
    def create(
        company_id: str,
        name: str,
        tags: Optional[Sequence[str]] = None,
        system_tags: Optional[Sequence[str]] = None,
        metadata: Optional[dict] = None,
    ) -> Queue:
        """Creates a queue"""
        with translate_errors_context():
            now = datetime.utcnow()
            queue = Queue(
                id=database.utils.id(),
                company=company_id,
                created=now,
                name=name,
                tags=tags or [],
                system_tags=system_tags or [],
                metadata=metadata,
                last_update=now,
            )
            queue.save()
            return queue

    def get_by_name(
        self,
        company_id: str,
        queue_name: str,
        only: Optional[Sequence[str]] = None,
    ) -> Queue:
        qs = Queue.objects(name=queue_name, company=company_id)
        if only:
            qs = qs.only(*only)

        return qs.first()

    def get_by_id(
        self, company_id: str, queue_id: str, only: Optional[Sequence[str]] = None
    ) -> Queue:
        """
        Get queue by id
        :raise errors.bad_request.InvalidQueueId: if the queue is not found
        """
        with translate_errors_context():
            query = dict(id=queue_id, company=company_id)
            qs = Queue.objects(**query)
            if only:
                qs = qs.only(*only)
            queue = qs.first()
            if not queue:
                raise errors.bad_request.InvalidQueueId(**query)

            return queue

    @classmethod
    def get_queue_with_task(cls, company_id: str, queue_id: str, task_id: str) -> Queue:
        with translate_errors_context():
            query = dict(id=queue_id, company=company_id)
            queue = Queue.objects(entries__task=task_id, **query).first()
            if not queue:
                raise errors.bad_request.InvalidQueueOrTaskNotQueued(
                    task=task_id, **query
                )

            return queue

    def get_default(self, company_id: str) -> Queue:
        """
        Get the default queue
        :raise errors.bad_request.NoDefaultQueue: if the default queue not found
        :raise errors.bad_request.MultipleDefaultQueues: if more than one default queue is found
        """
        with translate_errors_context():
            res = Queue.objects(company=company_id, system_tags="default").only(
                "id", "name"
            )
            if not res:
                raise errors.bad_request.NoDefaultQueue()
            if len(res) > 1:
                raise errors.bad_request.MultipleDefaultQueues(
                    queues=tuple(r.id for r in res)
                )

            return res.first()

    def update(
        self, company_id: str, queue_id: str, **update_fields
    ) -> Tuple[int, dict]:
        """
        Partial update of the queue from update_fields
        :raise errors.bad_request.InvalidQueueId: if the queue is not found
        :return: number of updated objects and updated fields dictionary
        """
        with translate_errors_context():
            # validate the queue exists
            self.get_by_id(company_id=company_id, queue_id=queue_id, only=("id",))
            return Queue.safe_update(company_id, queue_id, update_fields)

    def delete(self, company_id: str, queue_id: str, force: bool) -> None:
        """
        Delete the queue
        :raise errors.bad_request.InvalidQueueId: if the queue is not found
        :raise errors.bad_request.QueueNotEmpty: if the queue is not empty and 'force' not set
        """
        with translate_errors_context():
            queue = self.get_by_id(company_id=company_id, queue_id=queue_id)
            if queue.entries and not force:
                raise errors.bad_request.QueueNotEmpty(
                    "use force=true to delete", id=queue_id
                )
            queue.delete()

    def get_all(
        self,
        company_id: str,
        query_dict: dict,
        ret_params: dict = None,
    ) -> Sequence[dict]:
        """Get all the queues according to the query"""
        with translate_errors_context():
            return Queue.get_many(
                company=company_id,
                parameters=query_dict,
                query_dict=query_dict,
                ret_params=ret_params,
            )

    def get_queue_infos(
        self,
        company_id: str,
        query_dict: dict,
        ret_params: dict = None,
    ) -> Sequence[dict]:
        """
        Get infos on all the company queues, including queue tasks and workers
        """
        projection = Queue.get_extra_projection("entries.task.name")
        with translate_errors_context():
            res = Queue.get_many_with_join(
                company=company_id,
                query_dict=query_dict,
                override_projection=projection,
                ret_params=ret_params,
            )

            queue_workers = defaultdict(list)
            for worker in self.worker_bll.get_all(company_id):
                for queue in worker.queues:
                    queue_workers[queue].append(worker)

            for item in res:
                item["workers"] = [
                    {
                        "name": w.id,
                        "ip": w.ip,
                        "task": w.task.to_struct() if w.task else None,
                    }
                    for w in queue_workers.get(item["id"], [])
                ]

        return res

    def add_task(self, company_id: str, queue_id: str, task_id: str) -> dict:
        """
        Add the task to the queue and return the queue update results
        :raise errors.bad_request.TaskAlreadyQueued: if the task is already in the queue
        :raise errors.bad_request.InvalidQueueOrTaskNotQueued: if the queue update operation failed
        """
        with translate_errors_context():
            queue = self.get_by_id(company_id=company_id, queue_id=queue_id)
            if any(e.task == task_id for e in queue.entries):
                raise errors.bad_request.TaskAlreadyQueued(task=task_id)

            entry = Entry(added=datetime.utcnow(), task=task_id)
            query = dict(id=queue_id, company=company_id)
            res = Queue.objects(entries__task__ne=task_id, **query).update_one(
                push__entries=entry, last_update=datetime.utcnow(), upsert=False
            )

            queue.reload()
            self.metrics.log_queue_metrics_to_es(company_id=company_id, queues=[queue])

            if not res:
                raise errors.bad_request.InvalidQueueOrTaskNotQueued(
                    task=task_id, **query
                )

            return res

    def get_next_task(self, company_id: str, queue_id: str) -> Optional[Entry]:
        """
        Atomically pop and return the first task from the queue (or None)
        :raise errors.bad_request.InvalidQueueId: if the queue does not exist
        """
        with translate_errors_context():
            query = dict(id=queue_id, company=company_id)
            queue = Queue.objects(**query).modify(pop__entries=-1, upsert=False)
            if not queue:
                raise errors.bad_request.InvalidQueueId(**query)

            self.metrics.log_queue_metrics_to_es(company_id, queues=[queue])

            if not queue.entries:
                return

            try:
                Queue.objects(**query).update(last_update=datetime.utcnow())
            except Exception:
                log.exception("Error while updating Queue.last_update")

            return queue.entries[0]

    def remove_task(self, company_id: str, queue_id: str, task_id: str) -> int:
        """
        Removes the task from the queue and returns the number of removed items
        :raise errors.bad_request.InvalidQueueOrTaskNotQueued: if the task is not found in the queue
        """
        with translate_errors_context():
            queue = self.get_queue_with_task(
                company_id=company_id, queue_id=queue_id, task_id=task_id
            )

            entries_to_remove = [e for e in queue.entries if e.task == task_id]
            query = dict(id=queue_id, company=company_id)
            res = Queue.objects(entries__task=task_id, **query).update_one(
                pull_all__entries=entries_to_remove, last_update=datetime.utcnow()
            )

            queue.reload()
            self.metrics.log_queue_metrics_to_es(company_id=company_id, queues=[queue])

            return len(entries_to_remove) if res else 0

    def reposition_task(
        self,
        company_id: str,
        queue_id: str,
        task_id: str,
        pos_func: Callable[[int], int],
    ) -> int:
        """
        Moves the task in the queue to the position calculated by pos_func
        Returns the updated task position in the queue
        """
        with translate_errors_context():
            queue = self.get_queue_with_task(
                company_id=company_id, queue_id=queue_id, task_id=task_id
            )

            position = next(i for i, e in enumerate(queue.entries) if e.task == task_id)
            new_position = pos_func(position)

            if new_position != position:
                entry = queue.entries[position]
                query = dict(id=queue_id, company=company_id)
                updated = Queue.objects(entries__task=task_id, **query).update_one(
                    pull__entries=entry, last_update=datetime.utcnow()
                )
                if not updated:
                    raise errors.bad_request.RemovedDuringReposition(
                        task=task_id, **query
                    )
                inst = {"$push": {"entries": {"$each": [entry.to_proper_dict()]}}}
                if new_position >= 0:
                    inst["$push"]["entries"]["$position"] = new_position
                res = Queue.objects(entries__task__ne=task_id, **query).update_one(
                    __raw__=inst
                )
                if not res:
                    raise errors.bad_request.FailedAddingDuringReposition(
                        task=task_id, **query
                    )

            return new_position
