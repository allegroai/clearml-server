from apierrors.errors.bad_request import InvalidProjectId
from apierrors.errors.forbidden import NoWritePermission
from config import config
from tests.automated import TestService


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
