from time import sleep
from typing import Sequence, Optional, Tuple

from boltons.iterutils import first

from apiserver.apierrors import errors
from apiserver.database.model import EntityVisibility
from apiserver.database.utils import id as db_id
from apiserver.tests.automated import TestService


class TestSubProjects(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.13")

    def test_project_aggregations(self):
        child = self._temp_project(name="Aggregation/Pr1")
        project = self.api.projects.get_all_ex(name="^Aggregation$").projects[0].id
        child_project = self.api.projects.get_all_ex(id=[child]).projects[0]
        self.assertEqual(child_project.parent.id, project)

        user = self.api.users.get_current_user().user.id

        # test aggregations on project with empty subprojects
        res = self.api.users.get_all_ex(active_in_projects=[project])
        self.assertEqual(res.users, [])
        res = self.api.projects.get_all_ex(id=[project], active_users=[user])
        self.assertEqual(res.projects, [])
        res = self.api.models.get_frameworks(projects=[project])
        self.assertEqual(res.frameworks, [])
        res = self.api.tasks.get_types(projects=[project])
        self.assertEqual(res.types, [])
        res = self.api.projects.get_task_parents(projects=[project])
        self.assertEqual(res.parents, [])

        # test aggregations with non-empty subprojects
        task1 = self._temp_task(project=child)
        self._temp_task(project=child, parent=task1)
        framework = "Test framework"
        self._temp_model(project=child, framework=framework)
        res = self.api.users.get_all_ex(active_in_projects=[project])
        self._assert_ids(res.users, [user])
        res = self.api.projects.get_all_ex(id=[project], active_users=[user])
        self._assert_ids(res.projects, [project])
        res = self.api.projects.get_task_parents(projects=[project])
        self._assert_ids(res.parents, [task1])
        res = self.api.models.get_frameworks(projects=[project])
        self.assertEqual(res.frameworks, [framework])
        res = self.api.tasks.get_types(projects=[project])
        self.assertEqual(res.types, ["testing"])

    def _assert_ids(self, actual: Sequence[dict], expected: Sequence[str]):
        self.assertEqual([a["id"] for a in actual], expected)

    def test_project_operations(self):
        # create
        with self.api.raises(errors.bad_request.InvalidProjectName):
            self._temp_project(name="/")
        project1 = self._temp_project(name="Root1/Pr1")
        project1_child = self._temp_project(name="Root1/Pr1/Pr2")
        with self.api.raises(errors.bad_request.ExpectedUniqueData):
            self._temp_project(name="Root1/Pr1/Pr2")

        # update
        with self.api.raises(errors.bad_request.CannotUpdateProjectLocation):
            self.api.projects.update(project=project1, name="Root2/Pr2")
        res = self.api.projects.update(project=project1, name="Root1/Pr2")
        self.assertEqual(res.updated, 1)
        res = self.api.projects.get_by_id(project=project1_child)
        self.assertEqual(res.project.name, "Root1/Pr2/Pr2")

        # move
        res = self.api.projects.move(project=project1, new_location="Root2")
        self.assertEqual(res.moved, 2)
        res = self.api.projects.get_by_id(project=project1_child)
        self.assertEqual(res.project.name, "Root2/Pr2/Pr2")

        # merge
        project_with_task, (active, archived) = self._temp_project_with_tasks(
            "Root1/Pr3/Pr4"
        )
        project1_parent = self._getProjectParent(project1)
        self._assertTags(project1_parent, tags=[], system_tags=[])
        self._assertTags(project1_parent, tags=[], system_tags=[])
        project_with_task_parent = self._getProjectParent(project_with_task)
        self._assertTags(project_with_task_parent)
        # self._assertTags(project_id=None)

        merge_source = self.api.projects.get_by_id(
            project=project_with_task
        ).project.parent
        res = self.api.projects.merge(
            project=merge_source, destination_project=project1
        )
        self.assertEqual(res.moved_entities, 0)
        self.assertEqual(res.moved_projects, 1)
        res = self.api.projects.get_by_id(project=project_with_task)
        self.assertEqual(res.project.name, "Root2/Pr2/Pr4")
        with self.api.raises(errors.bad_request.InvalidProjectId):
            self.api.projects.get_by_id(project=merge_source)

        self._assertTags(project1_parent)
        self._assertTags(project1)
        self._assertTags(project_with_task_parent, tags=[], system_tags=[])
        # self._assertTags(project_id=None)

        # delete
        with self.api.raises(errors.bad_request.ProjectHasTasks):
            self.api.projects.delete(project=project1)
        res = self.api.projects.delete(project=project1, force=True)
        self.assertEqual(res.deleted, 3)
        self.assertEqual(res.disassociated_tasks, 2)
        res = self.api.tasks.get_by_id(task=active).task
        self.assertIsNone(res.get("project"))
        for p_id in (project1, project1_child, project_with_task):
            with self.api.raises(errors.bad_request.InvalidProjectId):
                self.api.projects.get_by_id(project=p_id)

        self._assertTags(project1_parent, tags=[], system_tags=[])
        # self._assertTags(project_id=None, tags=[], system_tags=[])

    def _getProjectParent(self, project_id: str):
        return self.api.projects.get_all_ex(id=[project_id]).projects[0].parent.id

    def _assertTags(
        self,
        project_id: Optional[str],
        tags: Sequence[str] = ("test",),
        system_tags: Sequence[str] = (EntityVisibility.archived.value,),
    ):
        if project_id:
            res = self.api.projects.get_task_tags(
                projects=[project_id], include_system=True
            )
        else:
            res = self.api.organization.get_tags(include_system=True)

        self.assertEqual(set(res.tags), set(tags))
        self.assertEqual(set(res.system_tags), set(system_tags))

    def test_get_all_search_options(self):
        project1 = self._temp_project(name="project1")
        project2 = self._temp_project(name="project1/project2")
        self._temp_project(name="project3")

        # local search finds only at the specified level
        res = self.api.projects.get_all_ex(
            name="project1", shallow_search=True
        ).projects
        self.assertEqual([p.id for p in res], [project1])
        res = self.api.projects.get_all_ex(name="project1", parent=[project1]).projects
        self.assertEqual([p.id for p in res], [project2])

        # global search finds all or below the specified level
        res = self.api.projects.get_all_ex(name="project1").projects
        self.assertEqual(set(p.id for p in res), {project1, project2})
        project4 = self._temp_project(name="project1/project2/project1")
        res = self.api.projects.get_all_ex(name="project1", parent=[project2]).projects
        self.assertEqual([p.id for p in res], [project4])

        self.api.projects.delete(project=project1, force=True)

    def test_get_all_with_stats(self):
        project4, _ = self._temp_project_with_tasks(name="project1/project3/project4")
        project5, _ = self._temp_project_with_tasks(name="project1/project3/project5")
        project2 = self._temp_project(name="project2")
        res = self.api.projects.get_all(shallow_search=True).projects
        self.assertTrue(any(p for p in res if p.id == project2))
        self.assertFalse(any(p for p in res if p.id in [project4, project5]))

        project1 = first(p.id for p in res if p.name == "project1")
        res = self.api.projects.get_all_ex(
            id=[project1, project2], include_stats=True
        ).projects
        self.assertEqual(set(p.id for p in res), {project1, project2})
        res1 = next(p for p in res if p.id == project1)
        self.assertEqual(res1.stats["active"]["status_count"]["created"], 0)
        self.assertEqual(res1.stats["active"]["status_count"]["stopped"], 2)
        self.assertEqual(res1.stats["active"]["total_runtime"], 2)
        self.assertEqual(
            {sp.name for sp in res1.sub_projects},
            {
                "project1/project3",
                "project1/project3/project4",
                "project1/project3/project5",
            },
        )
        res2 = next(p for p in res if p.id == project2)
        self.assertEqual(res2.stats["active"]["status_count"]["created"], 0)
        self.assertEqual(res2.stats["active"]["status_count"]["stopped"], 0)
        self.assertEqual(res2.stats["active"]["total_runtime"], 0)
        self.assertEqual(res2.sub_projects, [])

    def _run_tasks(self, *tasks):
        """Imitate 1 second of running"""
        for task_id in tasks:
            self.api.tasks.started(task=task_id)
        sleep(1)
        for task_id in tasks:
            self.api.tasks.stopped(task=task_id)

    def _temp_project_with_tasks(self, name) -> Tuple[str, Tuple[str, str]]:
        pr_id = self._temp_project(name=name)
        task_active = self._temp_task(project=pr_id)
        task_archived = self._temp_task(
            project=pr_id, system_tags=[EntityVisibility.archived.value], tags=["test"]
        )
        self._run_tasks(task_active, task_archived)
        return pr_id, (task_active, task_archived)

    delete_params = dict(can_fail=True, force=True)

    def _temp_project(self, name, **kwargs):
        return self.create_temp(
            "projects",
            delete_params=self.delete_params,
            name=name,
            description="",
            **kwargs,
        )

    def _temp_task(self, **kwargs):
        return self.create_temp(
            "tasks",
            delete_params=self.delete_params,
            type="testing",
            name=db_id(),
            input=dict(view=dict()),
            **kwargs,
        )

    def _temp_model(self, **kwargs):
        return self.create_temp(
            service="models",
            delete_params=self.delete_params,
            name="test",
            uri="file:///a",
            labels={},
            **kwargs,
        )
