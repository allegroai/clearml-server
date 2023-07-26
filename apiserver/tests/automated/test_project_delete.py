from apiserver.apierrors import errors
from apiserver.database.model import EntityVisibility
from apiserver.tests.automated import TestService
from apiserver.database.utils import id as db_id


class TestProjectsDelete(TestService):
    def new_task(self, type="testing", **kwargs):
        return self.create_temp(
            "tasks", type=type, name=db_id(), **kwargs
        )

    def new_model(self, **kwargs):
        return self.create_temp("models", uri="file:///a/b", name=db_id(), labels={}, **kwargs)

    def new_project(self, name=None, **kwargs):
        return self.create_temp("projects", name=name or db_id(), description="", **kwargs)

    def test_delete_fails_with_active_task(self):
        project = self.new_project()
        self.new_task(project=project)
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.tasks, 1)
        self.assertEqual(res.non_archived_tasks, 1)
        with self.api.raises(errors.bad_request.ProjectHasTasks):
            self.api.projects.delete(project=project)

    def test_delete_with_archived_task(self):
        project = self.new_project()
        self.new_task(project=project, system_tags=[EntityVisibility.archived.value])
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.tasks, 1)
        self.assertEqual(res.non_archived_tasks, 0)
        self.api.projects.delete(project=project)

    def test_delete_fails_with_active_model(self):
        project = self.new_project()
        self.new_model(project=project)
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.models, 1)
        self.assertEqual(res.non_archived_models, 1)
        with self.api.raises(errors.bad_request.ProjectHasModels):
            self.api.projects.delete(project=project)

    def test_delete_with_archived_model(self):
        project = self.new_project()
        self.new_model(project=project, system_tags=[EntityVisibility.archived.value])
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.models, 1)
        self.assertEqual(res.non_archived_models, 0)
        self.api.projects.delete(project=project)

    def test_delete_dataset(self):
        name = "Test datasets delete"
        project = self.new_project(name=name)
        dataset = self.new_project(f"{name}/.datasets/test dataset", system_tags=["dataset"])
        task = self.new_task(project=dataset, system_tags=["dataset"])
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.datasets, 1)
        with self.api.raises(errors.bad_request.ProjectHasDatasets):
            self.api.projects.delete(project=project)

        self.api.tasks.delete(task=task)
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.datasets, 0)
        self.api.projects.delete(project=project)

    def test_delete_pipeline(self):
        name = "Test pipelines delete"
        project = self.new_project(name=name)
        pipeline = self.new_project(f"{name}/.pipelines/test pipeline", system_tags=["pipeline"])
        task = self.new_task(project=pipeline, type="controller", system_tags=["pipeline"])
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.pipelines, 1)
        with self.api.raises(errors.bad_request.ProjectHasPipelines):
            self.api.projects.delete(project=project)

        self.api.tasks.edit(task=task, system_tags=[EntityVisibility.archived.value])
        res = self.api.projects.validate_delete(project=project)
        self.assertEqual(res.pipelines, 0)
        self.api.projects.delete(project=project)
