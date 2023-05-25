from datetime import datetime
from time import sleep
from typing import Sequence

from apiserver.apierrors.errors import bad_request
from apiserver.database.utils import partition_tags
from apiserver.tests.api_client import APIClient, AttrDict
from apiserver.tests.automated import TestService
from apiserver.config_repo import config

log = config.logger(__file__)


class TestTags(TestService):
    def setUp(self, version="2.4"):
        super().setUp(version)

    def testPartition(self):
        tags, system_tags = partition_tags("project", ["test"])
        self.assertTagsEqual(tags, ["test"])
        self.assertTagsEqual(system_tags, [])

        tags, system_tags = partition_tags("project", ["test", "archived"])
        self.assertTagsEqual(tags, ["test"])
        self.assertTagsEqual(system_tags, ["archived"])

        tags, system_tags = partition_tags("project", ["test", "archived"], ["custom"])
        self.assertTagsEqual(tags, ["test"])
        self.assertTagsEqual(system_tags, ["archived", "custom"])

        tags, system_tags = partition_tags(
            "task", ["test", "development", "annotator20", "Annotation"]
        )
        self.assertTagsEqual(tags, ["test"])
        self.assertTagsEqual(system_tags, ["development", "annotator20", "Annotation"])

    def testBackwardsCompatibility(self):
        new_api = self.api
        self.api = APIClient(base_url="http://localhost:8008/v2.2")
        entity_tags = {
            "model": "archived",
            "project": "public",
            "task": "development",
        }

        for name, system_tag in entity_tags.items():
            create_func = getattr(self, f"_temp_{name}")
            _id = create_func(tags=[system_tag, "test"])
            names = f"{name}s"

            # when accessed through the old api all the tags are in the tags field
            self.assertGetById(
                service=names, entity=name, _id=_id, tags=[system_tag, "test"]
            )
            entities = self._send(
                names, "get_all", name="Test tags", tags=[f"-{system_tag}"]
            )[names]
            self.assertNotFound(_id, entities)

            # when accessed through the new api the tags are in tags and system_tags fields
            self.assertGetById(
                service=names,
                entity=name,
                _id=_id,
                tags=["test"],
                system_tags=[system_tag],
                api=new_api,
            )

            # update operation, remove system tag through the old api
            self._send(names, "update", tags=["test"], **{name: _id})
            self.assertGetById(service=names, entity=name, _id=_id, tags=["test"])

    def testProjectTags(self):
        pr_id = self._temp_project(system_tags=["default"])

        # Test getting project with system tags
        projects = self.api.projects.get_all(name="Test tags").projects
        self.assertFound(pr_id, ["default"], projects)

        projects = self.api.projects.get_all(
            name="Test tags", system_tags=["default"]
        ).projects
        self.assertFound(pr_id, ["default"], projects)

        projects = self.api.projects.get_all(
            name="Test tags", system_tags=["-default"]
        ).projects
        self.assertNotFound(pr_id, projects)

        self.api.projects.update(project=pr_id, system_tags=[])
        projects = self.api.projects.get_all(
            name="Test tags", system_tags=["-default"]
        ).projects
        self.assertFound(pr_id, [], projects)

        # Test task statistics and delete
        task1_id = self._temp_task(
            name="Tags test1", project=pr_id, system_tags=["active"]
        )
        self._run_task(task1_id)
        task2_id = self._temp_task(
            name="Tags test2", project=pr_id, system_tags=["archived"]
        )
        projects = self.api.projects.get_all_ex(name="Test tags").projects
        self.assertFound(pr_id, [], projects)

        projects = self.api.projects.get_all_ex(
            name="Test tags", include_stats=True
        ).projects
        project = next(p for p in projects if p.id == pr_id)
        self.assertProjectStats(project)

        with self.api.raises(bad_request.ProjectHasTasks):
            self.api.projects.delete(project=pr_id)
        self.api.projects.delete(project=pr_id, force=True)

    def testModelTags(self):
        model_id = self._temp_model(system_tags=["default"])
        models = self.api.models.get_all_ex(
            name="Test tags", system_tags=["default"]
        ).models
        self.assertFound(model_id, ["default"], models)

        models = self.api.models.get_all_ex(
            name="Test tags", system_tags=["-default"]
        ).models
        self.assertNotFound(model_id, models)

        self.api.models.update(model=model_id, system_tags=[])
        models = self.api.models.get_all_ex(
            name="Test tags", system_tags=["-default"]
        ).models
        self.assertFound(model_id, [], models)

    def testQueueTags(self):
        q_id = self._temp_queue(system_tags=["default"])
        queues = self.api.queues.get_all_ex(
            name="Test tags", system_tags=["default"]
        ).queues
        self.assertFound(q_id, ["default"], queues)

        queues = self.api.queues.get_all_ex(
            name="Test tags", system_tags=["-default"]
        ).queues
        self.assertNotFound(q_id, queues)

        self.api.queues.update(queue=q_id, system_tags=[])
        queues = self.api.queues.get_all_ex(
            name="Test tags", system_tags=["-default"]
        ).queues
        self.assertFound(q_id, [], queues)

        # test default queue
        queues = self.api.queues.get_all(system_tags=["default"]).queues
        if queues:
            self.assertEqual(queues[0].id, self.api.queues.get_default().id)
        else:
            self.api.queues.update(queue=q_id, system_tags=["default"])
            self.assertEqual(q_id, self.api.queues.get_default().id)

    def testTaskTags(self):
        task_id = self._temp_task(
            name="Test tags", system_tags=["active"]
        )
        tasks = self.api.tasks.get_all_ex(
            name="Test tags", system_tags=["active"]
        ).tasks
        self.assertFound(task_id, ["active"], tasks)

        tasks = self.api.tasks.get_all_ex(
            name="Test tags", system_tags=["-active"]
        ).tasks
        self.assertNotFound(task_id, tasks)

        self.api.tasks.update(task=task_id, system_tags=[])
        tasks = self.api.tasks.get_all_ex(
            name="Test tags", system_tags=["-active"]
        ).tasks
        self.assertFound(task_id, [], tasks)

        # test development system tag
        self.api.tasks.started(task=task_id)
        self.api.workers.status_report(
            worker="Test tags",
            timestamp=int(datetime.utcnow().timestamp() * 1000),
            machine_stats=dict(memory_used=30),
            task=task_id,
        )
        self.api.tasks.stop(task=task_id)
        task = self.api.tasks.get_by_id(task=task_id).task
        self.assertEqual(task.status, "in_progress")
        self.api.tasks.update(task=task_id, system_tags=["development"])
        self.api.tasks.stop(task=task_id)
        task = self.api.tasks.get_by_id(task=task_id).task
        self.assertEqual(task.status, "stopped")

    def assertProjectStats(self, project: AttrDict):
        self.assertEqual(set(project.stats.keys()), {"active"})
        self.assertAlmostEqual(project.stats.active.total_runtime, 1, places=0)
        self.assertEqual(project.stats.active.completed_tasks_24h, 1)
        self.assertEqual(project.stats.active.total_tasks, 1)
        for status, count in project.stats.active.status_count.items():
            self.assertEqual(count, 1 if status == "stopped" else 0)

    def _run_task(self, task_id):
        """Imitate 1 second of running"""
        self.api.tasks.started(task=task_id)
        sleep(1)
        self.api.tasks.stopped(task=task_id)

    def _temp_queue(self, **kwargs):
        self.update_missing(kwargs, name="Test tags")
        return self.create_temp("queues", **kwargs)

    def _temp_project(self, **kwargs):
        self.update_missing(kwargs, name="Test tags", description="test")
        return self.create_temp("projects", **kwargs)

    def _temp_model(self, **kwargs):
        self.update_missing(kwargs, name="Test tags", uri="file:///a/b", labels={})
        return self.create_temp("models", **kwargs)

    def _temp_task(self, **kwargs):
        self.update_missing(kwargs, name="Test tags", type="testing")
        return self.create_temp("tasks", **kwargs)

    def _send(self, service, action, **kwargs):
        api = kwargs.pop("api", self.api)
        return AttrDict(
            api.send(f"{service}.{action}", kwargs)[1]
        )

    def assertGetById(self, service, entity, _id, tags, system_tags=None, **kwargs):
        entity = self._send(service, "get_by_id", **{entity: _id}, **kwargs)[entity]
        self.assertEqual(set(entity.tags), set(tags))
        if system_tags is not None:
            self.assertEqual(set(entity.system_tags), set(system_tags))

    def assertFound(
        self, _id: str, system_tags: Sequence[str], res: Sequence[AttrDict]
    ):
        found = next((r for r in res if _id == r.id), None)
        assert found
        self.assertTagsEqual(found.system_tags, system_tags)

    def assertNotFound(
        self, _id: str, res: Sequence[AttrDict]
    ):
        self.assertFalse(any(r for r in res if r.id == _id))

    def assertTagsEqual(self, tags: Sequence[str], expected_tags: Sequence[str]):
        self.assertEqual(set(tags), set(expected_tags))
