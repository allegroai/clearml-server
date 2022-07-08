from apiserver.apierrors.errors.bad_request import InvalidProjectId, ExpectedUniqueData
from apiserver.apierrors.errors.forbidden import NoWritePermission
from apiserver.config_repo import config
from apiserver.tests.automated import TestService


log = config.logger(__file__)


class TestProjectsEdit(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.9")

    def test_make_public(self):
        p1 = self.create_temp("projects", name="Test public", description="test")

        # project with company_origin not set to the current company cannot be converted to private
        with self.api.raises(InvalidProjectId):
            self.api.projects.make_private(ids=[p1])

        # public project can be retrieved but not updated
        res = self.api.projects.make_public(ids=[p1])
        self.assertEqual(res.updated, 1)
        res = self.api.projects.get_all(id=[p1])
        self.assertEqual([p.id for p in res.projects], [p1])
        with self.api.raises(NoWritePermission):
            self.api.projects.update(project=p1, name="Test public change 1")

        # task made private again and can be both retrieved and updated
        res = self.api.projects.make_private(ids=[p1])
        self.assertEqual(res.updated, 1)
        res = self.api.projects.get_all(id=[p1])
        self.assertEqual([p.id for p in res.projects], [p1])
        self.api.projects.update(project=p1, name="Test public change 2")

    def test_project_name_uniqueness(self):
        name1 = "Test name1"
        p1 = self.create_temp("projects", name=name1, description="test")
        with self.api.raises(ExpectedUniqueData):
            p2 = self.create_temp("projects", name=name1, description="test")
        p2 = self.create_temp("projects", name="Test name2", description="test")
        with self.api.raises(ExpectedUniqueData):
            self.api.projects.update(project=p2, name=name1)
