from typing import Set

from apiserver.apierrors import errors
from apiserver.es_factory import es_factory
from apiserver.tests.automated import TestService


class TestTasksResetDelete(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.11")

    def test_delete(self):
        # draft task can be deleted
        task = self.new_task()
        res = self.assert_delete_task(task)
        self.assertIsNone(res.get("urls"))
        # published task can be deleted only with force flag
        task = self.new_task()
        self.publish_task(task)
        with self.api.raises(errors.bad_request.TaskCannotBeDeleted):
            self.assert_delete_task(task)
        self.assert_delete_task(task, force=True)

        # task with published children can only be deleted with force flag
        task = self.new_task()
        child = self.new_task(parent=task)
        self.publish_task(child)
        with self.api.raises(errors.bad_request.TaskCannotBeDeleted):
            self.assert_delete_task(task)
        res = self.assert_delete_task(task, force=True)
        self.assertEqual(res.updated_children, 1)
        # make sure that the child model is valid after the parent deletion
        self.api.tasks.validate(**self.api.tasks.get_by_id(task=child).task)

        # task with published model can only be deleted with force flag
        task = self.new_task()
        model = self.new_model()
        self.api.models.edit(model=model, task=task, ready=True)
        with self.api.raises(errors.bad_request.TaskCannotBeDeleted):
            self.assert_delete_task(task)
        res = self.assert_delete_task(task, force=True)
        self.assertEqual(res.updated_models, 1)

    def test_return_file_urls(self):
        # empty task
        task = self.new_task()
        res = self.assert_delete_task(task, return_file_urls=True)
        self.assertEqual(res.urls.model_urls, [])
        self.assertEqual(res.urls.event_urls, [])
        self.assertEqual(res.urls.artifact_urls, [])

        task = self.new_task()
        model_urls = self.create_task_models(task)
        artifact_urls = self.send_artifacts(task)
        event_urls = self.send_debug_image_events(task)
        event_urls.update(self.send_plot_events(task))
        res = self.assert_delete_task(task, force=True, return_file_urls=True)
        self.assertEqual(set(res.urls.model_urls), model_urls)
        self.assertEqual(set(res.urls.event_urls), event_urls)
        self.assertEqual(set(res.urls.artifact_urls), artifact_urls)

    def test_reset(self):
        # draft task can be deleted
        task = self.new_task()
        res = self.api.tasks.reset(task=task)
        self.assertFalse(res.get("urls"))

        # published task can be reset only with force flag
        task = self.new_task()
        self.publish_task(task)
        with self.api.raises(errors.bad_request.InvalidTaskStatus):
            self.api.tasks.reset(task=task)
        self.api.tasks.reset(task=task, force=True)

        # test urls
        task = self.new_task()
        model_urls = self.create_task_models(task)
        artifact_urls = self.send_artifacts(task)
        event_urls = self.send_debug_image_events(task)
        event_urls.update(self.send_plot_events(task))
        res = self.api.tasks.reset(task=task, force=True, return_file_urls=True)
        self.assertEqual(set(res.urls.model_urls), model_urls)
        self.assertEqual(set(res.urls.event_urls), event_urls)
        self.assertEqual(set(res.urls.artifact_urls), artifact_urls)

    def test_model_delete(self):
        model = self.new_model(uri="test")
        res = self.api.models.delete(model=model, return_file_url=True)
        self.assertEqual(res.url, "test")

    def assert_delete_task(self, task_id, force=False, return_file_urls=False):
        tasks = self.api.tasks.get_all_ex(id=[task_id]).tasks
        self.assertEqual(tasks[0].id, task_id)
        res = self.api.tasks.delete(
            task=task_id, force=force, return_file_urls=return_file_urls
        )
        self.assertTrue(res.deleted)
        tasks = self.api.tasks.get_all_ex(id=[task_id]).tasks
        self.assertEqual(tasks, [])
        return res

    def create_task_models(self, task) -> Set[str]:
        """
        Update models from task and return only non public models
        """
        model_ready = self.new_model(uri="ready")
        model_not_ready = self.new_model(uri="not_ready", ready=False)
        self.api.models.edit(model=model_not_ready, task=task)
        self.api.models.edit(model=model_ready, task=task)
        return {"not_ready"}

    def send_artifacts(self, task) -> Set[str]:
        """
        Add input and output artifacts and return output artifact names
        """
        artifacts = [
            dict(key="a", type="str", uri="test1", mode="input"),
            dict(key="b", type="int", uri="test2"),
        ]
        # test create/get and get_all
        self.api.tasks.add_or_update_artifacts(task=task, artifacts=artifacts)
        return {"test2"}

    def send_debug_image_events(self, task) -> Set[str]:
        events = [
            self.create_event(task, "training_debug_image", iteration, url=f"url_{iteration}")
            for iteration in range(5)
        ]
        self.send_batch(events)
        return set(ev["url"] for ev in events)

    def send_plot_events(self, task) -> Set[str]:
        plots = [
            '{"data": [], "layout": {"xaxis": {"visible": false, "range": [0, 640]}, "yaxis": {"visible": false, "range": [0, 514], "scaleanchor": "x"}, "margin": {"l": 0, "r": 0, "t": 64, "b": 0}, "images": [{"sizex": 640, "sizey": 514, "xref": "x", "yref": "y", "opacity": 1.0, "x": 0, "y": 514, "sizing": "contain", "layer": "below", "source": "https://files.community-master.hosted.allegro.ai/examples/XGBoost%20simple%20example.35abd481a6ea4a6a976c217e80191dcd/metrics/Feature%20importance/plot%20image/Feature%20importance_plot%20image_00000000.png"}], "showlegend": false, "title": "Feature importance/plot image", "name": null}}',
            '{"data": [], "layout": {"xaxis": {"visible": false, "range": [0, 640]}, "yaxis": {"visible": false, "range": [0, 200], "scaleanchor": "x"}, "margin": {"l": 0, "r": 0, "t": 64, "b": 0}, "images": [{"sizex": 640, "sizey": 200, "xref": "x", "yref": "y", "opacity": 1.0, "x": 0, "y": 200, "sizing": "contain", "layer": "below", "source": "https://files.community-master.hosted.allegro.ai/examples/XGBoost%20simple%20example.35abd481a6ea4a6a976c217e80191dcd/metrics/untitled%2000/plot%20image/untitled%2000_plot%20image_00000000.jpeg"}], "showlegend": false, "title": "untitled 00/plot image", "name": null}}',
            '{"data": [{"y": ["lying", "sitting", "standing", "people", "backgroun"], "x": ["lying", "sitting", "standing", "people", "backgroun"], "z": [[758, 163, 0, 0, 23], [63, 858, 3, 0, 0], [0, 50, 188, 21, 35], [0, 22, 8, 40, 4], [12, 91, 26, 29, 368]], "type": "heatmap"}], "layout": {"title": "Confusion Matrix for iter 100", "xaxis": {"title": "Predicted value"}, "yaxis": {"title": "Real value"}}}',
        ]
        events = [
            self.create_event(task, "plot", iteration, plot_str=plot_str)
            for iteration, plot_str in enumerate(plots)
        ]
        self.send_batch(events)
        return {
            "https://files.community-master.hosted.allegro.ai/examples/XGBoost%20simple%20example.35abd481a6ea4a6a976c217e80191dcd/metrics/Feature%20importance/plot%20image/Feature%20importance_plot%20image_00000000.png",
            "https://files.community-master.hosted.allegro.ai/examples/XGBoost%20simple%20example.35abd481a6ea4a6a976c217e80191dcd/metrics/untitled%2000/plot%20image/untitled%2000_plot%20image_00000000.jpeg",
        }

    def create_event(self, task, type_, iteration, **kwargs) -> dict:
        return {
            "worker": "test",
            "type": type_,
            "task": task,
            "iter": iteration,
            "timestamp": es_factory.get_timestamp_millis(),
            "metric": "Metric1",
            "variant": "Variant1",
            **kwargs,
        }

    def send_batch(self, events):
        _, data = self.api.send_batch("events.add_batch", events)
        return data

    def new_task(self, **kwargs):
        return self.create_temp(
            "tasks",
            delete_params=dict(can_fail=True),
            type="testing",
            name="test task delete",
            input=dict(view=dict()),
            **kwargs,
        )

    def new_model(self, **kwargs):
        self.update_missing(kwargs, name="test", uri="file:///a/b", labels={})
        return self.create_temp(
            "models",
            delete_params=dict(can_fail=True),
            **kwargs,
        )

    def publish_task(self, task_id):
        self.api.tasks.started(task=task_id)
        self.api.tasks.stopped(task=task_id)
        self.api.tasks.publish(task=task_id)
