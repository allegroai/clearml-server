from apiserver.tests.automated import TestService


class TestTaskParent(TestService):
    def setUp(self, version="2.12"):
        super().setUp(version=version)

    def test_query_by_parents(self):
        parent = self.new_task()
        child = self.new_task(name="Test parent task1", parent=parent)
        tasks = self.api.tasks.get_all_ex(parent=[parent]).tasks
        self.assertEqual([t.id for t in tasks], [child])

        tasks = self.api.tasks.get_all(parent=parent).tasks
        self.assertEqual([t.id for t in tasks], [child])

    def test_query_by_project(self):
        # stand alone task
        parent_sa_name = "Test parent parent standalone"
        parent_sa = self.new_task(name=parent_sa_name)
        self.new_task(name="Test parent task standalone", parent=parent_sa)

        # tasks in projects
        project_name = "Test parents project"
        project = self.create_temp("projects", name=project_name, description="test")

        parent_name = "Test parent parent"
        parent = self.new_task(project=project, name=parent_name)

        self.new_task(project=project, name="Test parent task1", parent=parent)
        self.new_task(project=project, name="Test parent task2", parent=parent)

        parents = self.api.projects.get_task_parents(projects=[project]).parents
        self.assertEqual(
            [
                {
                    "id": parent,
                    "name": parent_name,
                    "project": {"id": project, "name": project_name},
                }
            ],
            parents,
        )

        res = self.api.projects.get_task_parents()
        parents = [p for p in res.parents if p.id in (parent, parent_sa)]
        self.assertEqual(
            [
                {
                    "id": parent,
                    "name": parent_name,
                    "project": {"id": project, "name": project_name},
                },
                {"id": parent_sa, "name": parent_sa_name},
            ],
            parents,
        )

    def test_query_by_name(self):
        project_name = "Test parents project"
        project = self.create_temp("projects", name=project_name, description="test")

        parent_names = [f"Parent{i}" for i in range(3)]
        parents = [self.new_task(project=project, name=name) for name in parent_names]

        for idx in range(2):
            self.new_task(project=project, name=f"Child{idx}", parent=parents[idx])

        parents = self.api.projects.get_task_parents(
            projects=[project], task_name="Parent"
        ).parents
        self.assertEqual(len(parents), 2)

        for parent_name in parent_names[:2]:
            res = self.api.projects.get_task_parents(
                projects=[project], task_name=parent_name
            ).parents
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0].name, parent_name)

        parents = self.api.projects.get_task_parents(
            projects=[project], task_name=parent_names[2]
        ).parents
        self.assertEqual(len(parents), 0)

    def test_query_by_state(self):
        project_name = "Test parents project"
        project = self.create_temp("projects", name=project_name, description="test")

        parent1_name = "Test parent parent1"
        parent1 = self.new_task(project=project, name=parent1_name)
        t1 = self.new_task(project=project, name="Test parent task1", parent=parent1)

        parent2_name = "Test parent parent2"
        parent2 = self.new_task(project=project, name=parent2_name)
        t2 = self.new_task(project=project, name="Test parent task2", parent=parent2)
        self.api.tasks.archive(tasks=[t2])

        # No state filter
        parents = self.api.projects.get_task_parents(projects=[project]).parents
        self.assertEqual([parent1, parent2], [p.id for p in parents])

        # Active tasks
        parents = self.api.projects.get_task_parents(
            projects=[project], tasks_state="active"
        ).parents
        self.assertEqual([parent1], [p.id for p in parents])

        # Archived tasks
        parents = self.api.projects.get_task_parents(
            projects=[project], tasks_state="archived"
        ).parents
        self.assertEqual([parent2], [p.id for p in parents])

    def new_task(self, **kwargs):
        self.update_missing(kwargs, type="testing", name="test task parents")
        return self.create_temp("tasks", **kwargs)
