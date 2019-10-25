import operator
from time import sleep

from typing import Sequence

from tests.automated import TestService


class TestEntityOrdering(TestService):
    test_comment = "Entity ordering test"
    only_fields = ["id", "started", "comment"]

    def setUp(self, **kwargs):
        super().setUp(**kwargs)
        self.task_ids = self._create_tasks()

    def test_order(self):
        # test no ordering
        self._assertGetTasksWithOrdering()

        # sort ascending
        self._assertGetTasksWithOrdering(order_by="started")

        # sort descending
        self._assertGetTasksWithOrdering(order_by="-started")

        # sort by the same field that we use for the search
        self._assertGetTasksWithOrdering(order_by="comment")

    def test_order_with_paging(self):
        order_field = "started"
        # all results in one page
        self._assertGetTasksWithOrdering(order_by=order_field, page=0, page_size=20)

        field_vals = []
        page_size = 2
        num_pages = 5
        for page in range(num_pages):
            paged_tasks = self._get_page_tasks(
                order_by=order_field, page=page, page_size=page_size
            )
            self.assertEqual(len(paged_tasks), page_size)
            field_vals.extend(t.get(order_field) for t in paged_tasks)

        paged_tasks = self._get_page_tasks(
            order_by=order_field, page=num_pages, page_size=page_size
        )
        self.assertTrue(not paged_tasks)

        self._assertSorted(field_vals)

    def _get_page_tasks(self, order_by, page: int, page_size: int) -> Sequence:
        return self.api.tasks.get_all_ex(
            only_fields=self.only_fields,
            order_by=order_by,
            comment=self.test_comment,
            page=page,
            page_size=page_size,
        ).tasks

    def _assertSorted(self, vals: Sequence, ascending=True):
        """
        Assert that vals are sorted in the ascending or descending order
        with None values are always coming from the end
        """
        if None in vals:
            first_null_idx = vals.index(None)
            none_tail = vals[first_null_idx:]
            vals = vals[:first_null_idx]
            self.assertTrue(all(val is None for val in none_tail))
            self.assertTrue(all(val is not None for val in vals))

        if ascending:
            cmp = operator.le
        else:
            cmp = operator.ge
        self.assertTrue(all(cmp(i, j) for i, j in zip(vals, vals[1:])))

    def _assertGetTasksWithOrdering(self, order_by: str = None, **kwargs):
        tasks = self.api.tasks.get_all_ex(
            only_fields=self.only_fields,
            order_by=order_by,
            comment=self.test_comment,
            **kwargs,
        ).tasks
        self.assertLessEqual(set(self.task_ids), set(t.id for t in tasks))
        if order_by:
            # test that the output is correctly ordered
            field_name = order_by if not order_by.startswith("-") else order_by[1:]
            field_vals = [t.get(field_name) for t in tasks]
            self._assertSorted(field_vals, ascending=not order_by.startswith("-"))

    def _create_tasks(self):
        tasks = [self._temp_task() for _ in range(10)]
        for _, task in zip(range(5), tasks):
            self.api.tasks.started(task=task)
            sleep(0.1)
        return tasks

    def _temp_task(self, **kwargs):
        return self.create_temp(
            "tasks",
            name="test",
            comment=self.test_comment,
            type="testing",
            input=dict(view=dict()),
            **kwargs,
        )
