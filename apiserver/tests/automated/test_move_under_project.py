from apiserver.tests.automated import TestService


class TestMoveUnderProject(TestService):
    entity_name = "test move"

    def setUp(self, version="2.12"):
        super().setUp(version=version)

    def test_move(self):
        # task move into the new project
        task = self._temp_task()
        project = self.api.tasks.move(ids=[task], project_name=self.entity_name).project_id
        tasks = self.api.tasks.get_all_ex(id=[task]).tasks
        self.assertEqual(project, tasks[0].project.id)
        projects = self.api.projects.get_all_ex(id=[project]).projects
        self.assertEqual(self.entity_name, projects[0].name)

        # task clone
        p2_name = "project_for_clone"
        res = self.api.tasks.clone(task=task, new_project_name=p2_name)
        task2 = res.id
        project_data = res.new_project
        self.assertTrue(project_data.id)
        self.assertEqual(p2_name, project_data.name)
        tasks = self.api.tasks.get_all_ex(id=[task2]).tasks
        project2 = tasks[0].project.id
        self.assertEqual(project_data.id, project2)
        projects = self.api.projects.get_all_ex(id=[project2]).projects
        self.assertEqual(p2_name, projects[0].name)
        self.api.projects.delete(project=project2, force=True)

        # move to the root project
        self.assertEqual(None, self.api.tasks.move(ids=[task], project=None).project_id)
        tasks = self.api.tasks.get_all_ex(id=[task]).tasks
        self.assertEqual(None, tasks[0].get("project"))

        # model move into existing project referenced by name
        model = self._temp_model()
        self.api.models.move(ids=[model], project_name=self.entity_name)
        models = self.api.models.get_all_ex(id=[model]).models
        self.assertEqual(project, models[0].project.id)

        self.api.projects.delete(project=project, force=True)

    def _temp_task(self):
        task_input = dict(
            name=self.entity_name, type="training"
        )
        return self.create_temp("tasks", **task_input)

    def _temp_model(self):
        model_input = dict(name=self.entity_name, uri="file:///a/b", labels={})
        return self.create_temp("models", **model_input)
