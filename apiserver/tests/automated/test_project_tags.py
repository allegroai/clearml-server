from datetime import datetime

from apiserver.tests.automated import TestService


class TestProjectTags(TestService):
    def test_project_own_tags(self):
        p1_tags = ["Tag 1", "Tag 2"]
        p1 = self.create_temp(
            "projects", name="Test project tags1", description="test", tags=p1_tags
        )
        p2_tags = ["Tag 1", "Tag 3"]
        p2 = self.create_temp(
            "projects",
            name="Test project tags2",
            description="test",
            tags=p2_tags,
            system_tags=["hidden"],
        )

        res = self.api.projects.get_project_tags(projects=[p1, p2])
        self.assertEqual(set(res.tags), set(p1_tags) | set(p2_tags))

        res = self.api.projects.get_project_tags(
            projects=[p1, p2], filter={"system_tags": ["__$not", "hidden"]}
        )
        self.assertEqual(res.tags, p1_tags)

    def test_project_entities_tags(self):
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
            kwargs, type="testing", name="test project tags"
        )
        return self.create_temp("tasks", **kwargs)

    def new_model(self, **kwargs):
        self.update_missing(kwargs, name="test project tags", uri="file:///a")
        return self.create_temp("models", **kwargs)
