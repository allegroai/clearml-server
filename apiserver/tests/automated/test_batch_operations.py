from apiserver.database.utils import id as db_id
from apiserver.tests.automated import TestService


class TestBatchOperations(TestService):
    name = "batch operation test"
    comment = "this is a comment"
    delete_params = dict(can_fail=True, force=True)

    def test_tasks(self):
        tasks = [self._temp_task() for _ in range(2)]
        models = [
            self._temp_task_model(task=t, uri=f"uri_{idx}")
            for idx, t in enumerate(tasks)
        ]
        missing_id = db_id()
        ids = [*tasks, missing_id]

        # enqueue
        res = self.api.tasks.enqueue_many(ids=ids, queue_name="test batch")
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"queued"})

        # stop
        for t in tasks:
            self.api.tasks.started(task=t)
        res = self.api.tasks.stop_many(ids=ids)
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"stopped"})

        # publish
        res = self.api.tasks.publish_many(ids=ids, publish_model=False)
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"published"})

        # reset
        res = self.api.tasks.reset_many(
            ids=ids, delete_output_models=True, return_file_urls=True, force=True
        )
        self._assert_succeeded(res, tasks)
        self.assertEqual(sum(t.deleted_models for t in res.succeeded), 2)
        self.assertEqual(
            set(url for t in res.succeeded for url in t.urls.model_urls),
            {"uri_0", "uri_1"},
        )
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertEqual({t.status for t in data}, {"created"})

        # archive/unarchive
        res = self.api.tasks.archive_many(ids=ids)
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertTrue(all("archived" in t.system_tags for t in data))

        res = self.api.tasks.unarchive_many(ids=ids)
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
        data = self.api.tasks.get_all_ex(id=ids).tasks
        self.assertFalse(any("archived" in t.system_tags for t in data))

        # delete
        res = self.api.tasks.delete_many(
            ids=ids, delete_output_models=True, return_file_urls=True
        )
        self._assert_succeeded(res, tasks)
        self._assert_failed(res, [missing_id])
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
        self._assert_succeeded(res, [ids[0]])
        self.assertEqual(res.succeeded[0].published_task.id, task)
        self._assert_failed(res, [ids[1], missing_id])

        # archive/unarchive
        res = self.api.models.archive_many(ids=ids)
        self._assert_succeeded(res, models)
        self._assert_failed(res, [missing_id])
        data = self.api.models.get_all_ex(id=ids).models
        self.assertTrue(all("archived" in m.system_tags for m in data))

        res = self.api.models.unarchive_many(ids=ids)
        self._assert_succeeded(res, models)
        self._assert_failed(res, [missing_id])
        data = self.api.models.get_all_ex(id=ids).models
        self.assertFalse(any("archived" in m.system_tags for m in data))

        # delete
        res = self.api.models.delete_many(ids=[*models, missing_id], force=True)
        self._assert_succeeded(res, models)
        self.assertEqual(set(m.url for m in res.succeeded), set(uris))
        self._assert_failed(res, [missing_id])
        data = self.api.models.get_all_ex(id=ids).models
        self.assertEqual(data, [])

    def _assert_succeeded(self, res, succeeded_ids):
        self.assertEqual(set(f.id for f in res.succeeded), set(succeeded_ids))

    def _assert_failed(self, res, failed_ids):
        self.assertEqual(set(f.id for f in res.failed), set(failed_ids))

    def _temp_model(self, **kwargs):
        self.update_missing(kwargs, name=self.name, uri="file:///a/b", labels={})
        return self.create_temp("models", delete_params=self.delete_params, **kwargs)

    def _temp_task(self):
        return self.create_temp(
            service="tasks", type="testing", name=self.name,
        )

    def _temp_task_model(self, task, **kwargs) -> str:
        model = self._temp_model(ready=False, task=task, **kwargs)
        self.api.tasks.add_or_update_model(
            task=task, name="output", type="output", model=model
        )
        return model
