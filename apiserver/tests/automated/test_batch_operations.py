from apiserver.database.utils import id as db_id
from apiserver.tests.automated import TestService


class TestBatchOperations(TestService):
    name = "batch operation test"
    comment = "this is a comment"
    delete_params = dict(can_fail=True, force=True)

    def setUp(self, version="2.13"):
        super().setUp(version=version)

    def test_tasks(self):
        tasks = [self._temp_task() for _ in range(2)]
        models = [
            self._temp_task_model(task=t, uri=f"uri_{idx}")
            for idx, t in enumerate(tasks)
        ]
        missing_id = db_id()
        ids = [*tasks, missing_id]

        # enqueue
        res = self.api.tasks.enqueue_many(ids=ids)
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"queued"})

        # stop
        for t in tasks:
            self.api.tasks.started(task=t)
        res = self.api.tasks.stop_many(ids=ids)
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"stopped"})

        # publish
        res = self.api.tasks.publish_many(ids=ids, publish_model=False)
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"published"})

        # reset
        res = self.api.tasks.reset_many(
            ids=ids, delete_output_models=True, return_file_urls=True, force=True
        )
        self.assertEqual(res.succeeded, 2)
        self.assertEqual(res.deleted_models, 2)
        self.assertEqual(set(res.urls.model_urls), {"uri_0", "uri_1"})
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"created"})

        # archive
        res = self.api.tasks.archive_many(ids=ids)
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertTrue(all("archived" in t.system_tags for t in data))

        # delete
        res = self.api.tasks.delete_many(
            ids=ids, delete_output_models=True, return_file_urls=True
        )
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual(data, [])

    def test_models(self):
        uris = [f"file:///{i}" for i in range(2)]
        models = [self._temp_model(uri=uri) for uri in uris]
        missing_id = db_id()
        ids = [*models, missing_id]

        # publish
        task = self._temp_task()
        self.api.models.edit(model=ids[0], ready=False, task=task)
        self.api.tasks.add_or_update_model(
            task=task, name="output", type="input", model=ids[0]
        )
        res = self.api.models.publish_many(
            ids=ids, publish_task=True, force_publish_task=True
        )
        self.assertEqual(res.succeeded, 1)
        self.assertEqual(res.published_tasks[0].id, task)
        self._assert_failures(res, [ids[1], missing_id])

        # archive
        res = self.api.models.archive_many(ids=ids)
        self.assertEqual(res.succeeded, 2)
        self._assert_failures(res, [missing_id])
        data = self.api.models.get_all_ex(id=ids).models
        for m in data:
            self.assertIn("archived", m.system_tags)

        # delete
        res = self.api.models.delete_many(ids=[*models, missing_id], force=True)
        self.assertEqual(res.succeeded, 2)
        self.assertEqual(set(res.urls), set(uris))
        self._assert_failures(res, [missing_id])
        data = self.api.models.get_all_ex(id=ids).models
        self.assertEqual(data, [])

    def _assert_failures(self, res, failed_ids):
        self.assertEqual(set(f.id for f in res.failures), set(failed_ids))

    def _temp_model(self, **kwargs):
        self.update_missing(kwargs, name=self.name, uri="file:///a/b", labels={})
        return self.create_temp("models", delete_params=self.delete_params, **kwargs)

    def _temp_task(self):
        return self.create_temp(
            service="tasks", type="testing", name=self.name, input=dict(view={}),
        )

    def _temp_task_model(self, task, **kwargs) -> str:
        model = self._temp_model(ready=False, task=task, **kwargs)
        self.api.tasks.add_or_update_model(
            task=task, name="output", type="output", model=model
        )
        return model
