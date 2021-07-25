from apiserver.apierrors import errors
from apiserver.database.model import EntityVisibility
from apiserver.tests.automated import TestService
from apiserver.database.utils import id as db_id


class TestProjectsDelete(TestService):
    def setUp(self, version="2.14"):
        super().setUp(version=version)

    def new_task(self, **kwargs):
        return self.create_temp(
            "tasks", type="testing", name=db_id(), input=dict(view=dict()), **kwargs
        )

    def new_model(self, **kwargs):
        return self.create_temp("models", uri="file:///a/b", name=db_id(), labels={}, **kwargs)

    def new_project(self, **kwargs):
        return self.create_temp("projects", name=db_id(), description="", **kwargs)

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
