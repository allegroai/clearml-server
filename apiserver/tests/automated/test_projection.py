from apiserver.config_repo import config
from apiserver.database.model.task.task import TaskStatus
from apiserver.tests.automated import TestService

log = config.logger(__file__)


class TestProjection(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.6")

    def _temp_task(self, **kwargs):
        self.update_missing(
            kwargs,
            type="testing",
            name="test projection",
            delete_params=dict(force=True),
        )
        return self.create_temp("tasks", **kwargs)

    def _temp_project(self):
        return self.create_temp(
            "projects",
            name="Test projection",
            description="test",
            delete_params=dict(force=True),
        )

    def test_overlapping_fields(self):
        message = "task started"
        task_id = self._temp_task()
        self.api.tasks.started(task=task_id, status_message=message)
        task = self.api.tasks.get_all_ex(
            id=[task_id], only_fields=["status", "status_message"]
        ).tasks[0]
        assert task["status"] == TaskStatus.in_progress
        assert task["status_message"] == message

    def test_task_projection(self):
        project = self._temp_project()
        task1 = self._temp_task(project=project)
        task2 = self._temp_task(project=project)
        self.api.tasks.started(task=task2, status_message="Started")

        res = self.api.tasks.get_all_ex(
            project=[project],
            only_fields=[
                "system_tags",
                "company",
                "type",
                "name",
                "tags",
                "status",
                "project.name",
                "user.name",
                "started",
                "last_update",
                "last_iteration",
                "comment",
            ],
            order_by=["-started"],
            page=0,
            page_size=15,
            system_tags=["-archived"],
            type=[
                "__$not",
                "annotation_manual",
                "__$not",
                "annotation",
                "__$not",
                "dataset_import",
            ],
        ).tasks
        self.assertEqual([task2, task1], [t.id for t in res])
        self.assertEqual("Test projection", res[0].project.name)

    def test_exclude_projection(self):
        task_id = self._temp_task()

        res = self.api.tasks.get_all_ex(
            id=[task_id]
        ).tasks[0]
        self.assertEqual("test projection", res.name)

        task = self.api.tasks.get_all_ex(
            id=[task_id],
            only_fields=["-name"]
        ).tasks[0]
        self.assertFalse("name" in task)
        self.assertEqual("testing", res.type)
