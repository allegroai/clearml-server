from config import config
from database.model.task.task import TaskStatus
from tests.automated import TestService

log = config.logger(__file__)


class TestProjection(TestService):
    def test_overlapping_fields(self):
        message = "task started"
        task_id = self.create_temp(
            "tasks", name="test", type="testing", input=dict(view=dict())
        )
        self.api.tasks.started(task=task_id, status_message=message)
        task = self.api.tasks.get_all_ex(
            id=[task_id], only_fields=["status", "status_message"]
        ).tasks[0]
        assert task["status"] == TaskStatus.in_progress
        assert task["status_message"] == message
