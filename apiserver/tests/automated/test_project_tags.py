from datetime import datetime

from apiserver.tests.automated import TestService


class TestProjectTags(TestService):
    def setUp(self, version="2.12"):
        super().setUp(version=version)

    def test_task_parent(self):
        # stand alone task
        parent_sa_name = "Test parent parent standalone"
        parent_sa = self.new_task(name=parent_sa_name)
        self.new_task(name="Test parent task standalone", parent=parent_sa)

        # tasks in projects
        parent_name = "Test parent parent"
        parent = self.new_task(name=parent_name)
        p1 = self.create_temp("projects", name="Test parents1", description="test")
        self.new_task(project=p1, name="Test parent task1", parent=parent)
        p2 = self.create_temp("projects", name="Test parents2", description="test")
        self.new_task(project=p1, name="Test parent task2", parent=parent)

        parents = self.api.projects.get_task_parents(projects=[p1, p2]).parents
        self.assertEqual([{"id": parent, "name": parent_name}], parents)

        res = self.api.projects.get_task_parents()
        parents = [p for p in res.parents if p.id in (parent, parent_sa)]
        self.assertEqual(
            [
                {"id": parent, "name": parent_name},
                {"id": parent_sa, "name": parent_sa_name},
            ],
            parents,
        )

    def test_project_tags(self):
        tags_1 = ["Test tag 1", "Test tag 2"]
        tags_2 = ["Test tag 3", "Test tag 4"]

        p1 = self.create_temp("projects", name="Test tags1", description="test")
        task1_1 = self.new_task(project=p1, tags=tags_1[:1])
        task1_2 = self.new_task(project=p1, tags=tags_1[1:])

        p2 = self.create_temp("projects", name="Test tasks2", description="test")
        task2 = self.new_task(project=p2, tags=tags_2)

        # test tags per project
        data = self.api.projects.get_task_tags(projects=[p1])
        self.assertEqual(set(tags_1), set(data.tags))
        data = self.api.projects.get_model_tags(projects=[p1])
        self.assertEqual(set(), set(data.tags))
        data = self.api.projects.get_task_tags(projects=[p2])
        self.assertEqual(set(tags_2), set(data.tags))

        # test tags for projects list
        data = self.api.projects.get_task_tags(projects=[p1, p2])
        self.assertEqual(set(tags_1) | set(tags_2), set(data.tags))

        # test tags for all projects
        data = self.api.projects.get_task_tags(projects=[p1, p2])
        self.assertTrue((set(tags_1) | set(tags_2)).issubset(data.tags))

        # test move to another project
        self.api.tasks.edit(task=task1_2, project=p2)
        data = self.api.projects.get_task_tags(projects=[p1])
        self.assertEqual(set(tags_1[:1]), set(data.tags))
        data = self.api.projects.get_task_tags(projects=[p2])
        self.assertEqual(set(tags_1[1:]) | set(tags_2), set(data.tags))

        # test tags update
        self.api.tasks.delete(task=task1_1, force=True)
        self.api.tasks.delete(task=task2, force=True)
        data = self.api.projects.get_task_tags(projects=[p1, p2])
        self.assertEqual(set(tags_1[1:]), set(data.tags))

    def test_organization_tags(self):
        tag1 = datetime.utcnow().isoformat()
        tag2 = "Orgtest tag2"
        system_tag = "Orgtest system tag"

        model = self.new_model(tags=[tag1])
        task = self.new_task(tags=[tag1])
        data = self.api.organization.get_tags()
        self.assertTrue(tag1 in data.tags)

        self.api.tasks.edit(task=task, tags=[tag2], system_tags=[system_tag])
        data = self.api.organization.get_tags(include_system=True)
        self.assertTrue({tag1, tag2}.issubset(set(data.tags)))
        self.assertTrue(system_tag in data.system_tags)

        data = self.api.organization.get_tags(
            filter={"system_tags": ["__$not", system_tag]}
        )
        self.assertTrue(tag1 in data.tags)
        self.assertFalse(tag2 in data.tags)

        self.api.models.delete(model=model)
        data = self.api.organization.get_tags()
        self.assertFalse(tag1 in data.tags)
        self.assertTrue(tag2 in data.tags)

    def new_task(self, **kwargs):
        self.update_missing(
            kwargs, type="testing", name="test project tags", input=dict(view=dict())
        )
        return self.create_temp("tasks", **kwargs)

    def new_model(self, **kwargs):
        self.update_missing(kwargs, name="test project tags", uri="file:///a")
        return self.create_temp("models", **kwargs)
