from boltons.iterutils import first

from apiserver.tests.automated import TestService


class TestProjectsRetrieval(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.13")

    def test_active_user(self):
        user = self.api.users.get_current_user().user.id
        project1 = self.temp_project(name="Project retrieval1")
        project2 = self.temp_project(name="Project retrieval2")
        self.temp_task(project=project2)

        projects = self.api.projects.get_all_ex().projects
        self.assertTrue({project1, project2}.issubset({p.id for p in projects}))

        projects = self.api.projects.get_all_ex(active_users=[user]).projects
        ids = {p.id for p in projects}
        self.assertFalse(project1 in ids)
        self.assertTrue(project2 in ids)

    def test_stats(self):
        project = self.temp_project()
        self.temp_task(project=project)
        self.temp_task(project=project)
        archived_task = self.temp_task(project=project)
        self.api.tasks.archive(tasks=[archived_task])

        p = self._get_project(project)
        self.assertFalse("stats" in p)

        p = self._get_project(project, include_stats=True)
        self.assertFalse("archived" in p.stats)
        self.assertTrue(p.stats.active.status_count.created, 2)

        p = self._get_project(project, include_stats=True, stats_for_state=None)
        self.assertTrue(p.stats.active.status_count.created, 2)
        self.assertTrue(p.stats.archived.status_count.created, 1)

    def _get_project(self, project, **kwargs):
        projects = self.api.projects.get_all_ex(id=[project], **kwargs).projects
        p = first(p for p in projects if p.id == project)
        self.assertIsNotNone(p)
        return p

    def temp_project(self, **kwargs) -> str:
        self.update_missing(
            kwargs,
            name="Test projects retrieval",
            description="test",
            delete_params=dict(force=True),
        )
        return self.create_temp("projects", **kwargs)

    def temp_task(self, **kwargs) -> str:
        self.update_missing(
            kwargs,
            type="testing",
            name="test projects retrieval",
            input=dict(view=dict()),
            delete_params=dict(force=True),
        )
        return self.create_temp("tasks", **kwargs)
