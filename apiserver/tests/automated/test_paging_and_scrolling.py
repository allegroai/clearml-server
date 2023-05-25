import math
from apiserver.tests.automated import TestService


class TestPagingAndScrolling(TestService):
    name_prefix = f"Test paging "

    def setUp(self, **kwargs):
        super().setUp(**kwargs)
        self.task_ids = self._create_tasks()

    def _create_tasks(self):
        tasks = [
            self._temp_task(
                name=f"{self.name_prefix}{i}",
                hyperparams={
                    "test": {
                        "param": {
                            "section": "test",
                            "name": "param",
                            "type": "str",
                            "value": str(i),
                        }
                    }
                },
            )
            for i in range(18)
        ]
        return tasks

    def test_paging(self):
        page_size = 10
        for page in range(0, math.ceil(len(self.task_ids) / page_size)):
            start = page * page_size
            expected_size = min(page_size, len(self.task_ids) - start)
            tasks = self._get_tasks(page=page, page_size=page_size,).tasks
            self.assertEqual(len(tasks), expected_size)
            for i, t in enumerate(tasks):
                self.assertEqual(t.name, f"{self.name_prefix}{start + i}")

    def test_scrolling(self):
        page_size = 10
        scroll_id = None
        for page in range(0, math.ceil(len(self.task_ids) / page_size)):
            start = page * page_size
            expected_size = min(page_size, len(self.task_ids) - start)
            res = self._get_tasks(size=page_size, scroll_id=scroll_id,)
            self.assertTrue(res.scroll_id)
            scroll_id = res.scroll_id
            tasks = res.tasks
            self.assertEqual(len(tasks), expected_size)
            for i, t in enumerate(tasks):
                self.assertEqual(t.name, f"{self.name_prefix}{start + i}")

        # no more data in this scroll
        tasks = self._get_tasks(size=page_size, scroll_id=scroll_id,).tasks
        self.assertFalse(tasks)

        # refresh brings all
        tasks = self._get_tasks(
            size=page_size, scroll_id=scroll_id, refresh_scroll=True,
        ).tasks
        self.assertEqual([t.id for t in tasks], self.task_ids)

    def _get_tasks(self, **page_params):
        return self.api.tasks.get_all_ex(
            name="^Test paging ",
            order_by=["hyperparams.test.param.value"],
            **page_params,
        )

    def _temp_task(self, name, **kwargs):
        return self.create_temp(
            "tasks",
            name=name,
            comment="Test task",
            type="testing",
            **kwargs,
        )
