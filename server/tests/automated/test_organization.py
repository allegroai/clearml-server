from tests.automated import TestService


class TestOrganization(TestService):
    def setUp(self, version="2.8"):
        super().setUp(version=version)

    def test_tags(self):
        tag1 = "Orgtest tag1"
        tag2 = "Orgtest tag2"
        system_tag = "Orgtest system tag"

        model = self.create_temp(
            "models", name="test_org", uri="file:///a", tags=[tag1]
        )
        task = self.create_temp(
            "tasks", name="test org", type="training", input=dict(view={}), tags=[tag1]
        )
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
