from apiserver.apierrors import errors
from apiserver.tests.automated import TestService


class TestGetAllExFilters(TestService):
    def test_list_filters(self):
        tags = ["a", "b", "c", "d"]
        tasks = [self._temp_task(tags=tags[:i]) for i in range(len(tags) + 1)]

        # invalid params check
        with self.api.raises(errors.bad_request.ValidationError):
            self.api.tasks.get_all_ex(filters={"tags": {"test": ["1"]}})

        # test any condition
        res = self.api.tasks.get_all_ex(
            id=tasks, filters={"tags": {"any": {"include": ["a", "b"]}}}
        ).tasks
        self.assertEqual(set(tasks[1:]), set(t.id for t in res))

        res = self.api.tasks.get_all_ex(
            id=tasks, filters={"tags": {"any": {"exclude": ["c", "d"]}}}
        ).tasks
        self.assertEqual(set(tasks[:-1]), set(t.id for t in res))

        res = self.api.tasks.get_all_ex(
            id=tasks,
            filters={"tags": {"any": {"include": ["a", "b"], "exclude": ["c", "d"]}}},
        ).tasks
        self.assertEqual(set(tasks), set(t.id for t in res))

        # test all condition
        res = self.api.tasks.get_all_ex(
            id=tasks, filters={"tags": {"all": {"include": ["a", "b"]}}}
        ).tasks
        self.assertEqual(set(tasks[2:]), set(t.id for t in res))

        res = self.api.tasks.get_all_ex(
            id=tasks, filters={"tags": {"all": {"exclude": ["c", "d"]}}}
        ).tasks
        self.assertEqual(set(tasks[:-2]), set(t.id for t in res))

        res = self.api.tasks.get_all_ex(
            id=tasks,
            filters={"tags": {"all": {"include": ["a", "b"], "exclude": ["c", "d"]}}},
        ).tasks
        self.assertEqual([tasks[2]], [t.id for t in res])

        # test combination
        res = self.api.tasks.get_all_ex(
            id=tasks,
            filters={
                "tags": {"any": {"include": ["a", "b"]}, "all": {"exclude": ["c", "d"]}}
            },
        ).tasks
        self.assertEqual(set(tasks[1:-2]), set(t.id for t in res))

    def _temp_task(self, **kwargs):
        self.update_missing(
            kwargs,
            name="test get_all_ex filters",
            type="training",
        )
        return self.create_temp(
            "tasks",
            **kwargs,
            delete_paramse=dict(can_fail=True, force=True),
        )
