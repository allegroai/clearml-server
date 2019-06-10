from tests.automated import TestService

MODEL_CANNOT_BE_UPDATED_CODES = (400, 203)
TASK_CANNOT_BE_UPDATED_CODES = (400, 110)
PUBLISHED = "published"
IN_PROGRESS = "in_progress"


class TestModelsService(TestService):
    def test_publish_output_model_running_task(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        with self._assert_update_task_failure():
            self.api.models.set_ready(model=model_id)

        self._assert_model_ready(model_id, False)
        self._assert_task_status(task_id, IN_PROGRESS)

    def test_publish_output_model_running_task_no_task_publish(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        res = self.api.models.set_ready(model=model_id, publish_task=False)
        assert res.updated == 1  # model updated
        assert res.get("published_task", None) is None

        self._assert_model_ready(model_id, True)
        self._assert_task_status(task_id, IN_PROGRESS)

    def test_publish_output_model_running_task_force_task_publish(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        self.api.models.set_ready(model=model_id, force_publish_task=True)

        self._assert_model_ready(model_id, True)
        self._assert_task_status(task_id, PUBLISHED)

    def test_publish_output_model_published_task(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        self.api.tasks.stopped(task=task_id)
        self.api.tasks.publish(task=task_id, publish_model=False)
        self._assert_model_ready(model_id, False)
        self._assert_task_status(task_id, PUBLISHED)

        res = self.api.models.set_ready(model=model_id)
        assert res.updated == 1  # model updated
        assert res.get("published_task", None) is None

        self._assert_model_ready(model_id, True)
        self._assert_task_status(task_id, PUBLISHED)

    def test_publish_output_model_stopped_task(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        self.api.tasks.stopped(task=task_id)

        res = self.api.models.set_ready(model=model_id)
        assert res.updated == 1  # model updated
        assert res.published_task.id == task_id
        assert res.published_task.data.updated == 1
        self._assert_model_ready(model_id, True)
        self._assert_task_status(task_id, PUBLISHED)

        # cannot publish already published model
        with self._assert_update_model_failure():
            self.api.models.set_ready(model=model_id)
        self._assert_model_ready(model_id, True)

    def test_publish_output_model_no_task(self):
        model_id = self.create_temp(
            service="models", name='test', uri='file:///a', labels={}, ready=False
        )
        self._assert_model_ready(model_id, False)

        res = self.api.models.set_ready(model=model_id)
        assert res.updated == 1  # model updated
        assert res.get("task", None) is None
        self._assert_model_ready(model_id, True)

    def test_publish_task_with_output_model(self):
        task_id, model_id = self._create_task_and_model()
        self._assert_model_ready(model_id, False)

        self.api.tasks.stopped(task=task_id)
        res = self.api.tasks.publish(task=task_id)
        assert res.updated == 1  # model updated
        self._assert_model_ready(model_id, True)
        self._assert_task_status(task_id, PUBLISHED)

    def test_publish_task_with_published_model(self):
        task_id, model_id = self._create_task_and_model()
        self.api.models.set_ready(model=model_id, publish_task=False)
        self._assert_model_ready(model_id, True)

        self.api.tasks.stopped(task=task_id)
        res = self.api.tasks.publish(task=task_id)
        assert res.updated == 1  # model updated
        self._assert_task_status(task_id, PUBLISHED)
        self._assert_model_ready(model_id, True)

    def test_publish_task_no_output_model(self):
        task_id = self.create_temp(
            service="tasks", type='testing', name='server-test', input=dict(view={})
        )
        self.api.tasks.started(task=task_id)
        self.api.tasks.stopped(task=task_id)

        res = self.api.tasks.publish(task=task_id)
        assert res.updated == 1  # model updated
        self._assert_task_status(task_id, PUBLISHED)

    def test_update_model_iteration_with_task(self):
        task_id = self._create_task()
        model_id = self._create_model()
        self.api.models.update(model=model_id, task=task_id, iteration=1000, labels={"foo": 1})

        self.assertEqual(
            self.api.tasks.get_by_id(task=task_id).task.last_iteration,
            1000
        )

        self.api.models.update(model=model_id, task=task_id, iteration=500)

        self.assertEqual(
            self.api.tasks.get_by_id(task=task_id).task.last_iteration,
            1000
        )

    def test_update_model_for_task_iteration(self):
        task_id = self._create_task()

        res = self.api.models.update_for_task(
            task=task_id,
            name="test model",
            uri="file:///b",
            iteration=999,
        )

        model_id = res.id

        self.defer(self.api.models.delete, can_fail=True, model=model_id, force=True)

        self.assertEqual(
            self.api.tasks.get_by_id(task=task_id).task.last_iteration,
            999
        )

        self.api.models.update_for_task(task=task_id, uri="file:///c", iteration=1000)

        self.assertEqual(
            self.api.tasks.get_by_id(task=task_id).task.last_iteration,
            1000
        )

        self.api.models.update_for_task(task=task_id, uri="file:///d", iteration=888)

        self.assertEqual(
            self.api.tasks.get_by_id(task=task_id).task.last_iteration,
            1000
        )

    def _assert_task_status(self, task_id, status):
        task = self.api.tasks.get_by_id(task=task_id).task
        assert task.status == status

    def _assert_model_ready(self, model_id, ready):
        model = self.api.models.get_by_id(model=model_id)["model"]
        assert model.ready == ready

    def _assert_update_model_failure(self):
        return self.api.raises(MODEL_CANNOT_BE_UPDATED_CODES)

    def _assert_update_task_failure(self):
        return self.api.raises(TASK_CANNOT_BE_UPDATED_CODES)

    def _create_model(self):
        model_id = self.create_temp(
            service="models",
            name='test',
            uri='file:///a',
            labels={}
        )

        self.defer(self.api.models.delete, can_fail=True, model=model_id, force=True)

        return model_id

    def _create_task(self):
        task_id = self.create_temp(
            service="tasks",
            type='testing',
            name='server-test',
            input=dict(view={}),
        )

        return task_id

    def _create_task_and_model(self):
        execution_model_id = self.create_temp(
            service="models",
            name='test',
            uri='file:///a',
            labels={}
        )
        task_id = self.create_temp(
            service="tasks",
            type='testing',
            name='server-test',
            input=dict(view={}),
            execution=dict(model=execution_model_id)
        )
        self.api.tasks.started(task=task_id)
        output_model_id = self.api.models.update_for_task(
            task=task_id, uri='file:///b'
        )["id"]

        return task_id, output_model_id
