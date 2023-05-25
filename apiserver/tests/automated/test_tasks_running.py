import time

from apiserver.tests.automated import TestService


class TestTasksRunning(TestService):
    STATUS_STOPPED = "stopped"
    STATUS_COMPLETED = "completed"
    STATUS_PUBLISHED = "published"
    STATUS_RUNNING = "in_progress"

    def test_stop_regular_task(self):
        task_id = self._create_running_task()
        data = self.api.tasks.stop(task=task_id).fields
        assert data.status == self.STATUS_STOPPED

    def test_stop_regular_task_with_active_worker(self):
        task_id = self._create_running_task()
        worker_id = "worker1"
        self.api.workers.register(worker=worker_id)
        self.api.workers.status_report(
            worker=worker_id, task=task_id, timestamp=int(time.time())
        )
        data = self.api.tasks.stop(task=task_id).fields
        assert data.status == self.STATUS_RUNNING
        assert data.status_message == "stopping"

    def test_stop_development_task(self):
        task_id = self._create_running_task(is_development=True)
        data = self.api.tasks.stop(task=task_id).fields
        assert data.status == self.STATUS_STOPPED

    def test_completed_task(self):
        task_id = self._create_running_task()
        res = self.api.tasks.completed(task=task_id)
        assert res.fields.status == self.STATUS_COMPLETED
        assert res.updated == 1
        assert res.published == 0

        res = self.api.tasks.completed(task=task_id, publish=True)
        assert res.fields.status == self.STATUS_PUBLISHED
        assert res.updated == 1
        assert res.published == 1

    def _create_running_task(self, is_development=False):
        task_input = dict(
            name="task-1",
            type="testing",
        )
        if is_development:
            task_input["system_tags"] = ["development"]

        task_id = self.create_temp("tasks", **task_input)

        self.api.tasks.started(task=task_id)
        return task_id

