from functools import partial
from typing import Sequence


from apiserver.es_factory import es_factory
from apiserver.tests.automated import TestService


class TestTaskDebugImages(TestService):
    def setUp(self, version="2.11"):
        super().setUp(version=version)

    def _temp_task(self, name="test task events"):
        task_input = dict(
            name=name, type="training", input=dict(mapping={}, view=dict(entries=[])),
        )
        return self.create_temp("tasks", **task_input)

    @staticmethod
    def _create_task_event(task, iteration, **kwargs):
        return {
            "worker": "test",
            "type": "training_debug_image",
            "task": task,
            "iter": iteration,
            "timestamp": kwargs.get("timestamp") or es_factory.get_timestamp_millis(),
            **kwargs,
        }

    def test_get_debug_image(self):
        task = self._temp_task()
        metric = "Metric1"
        variant = "Variant1"

        # test empty
        res = self.api.events.get_debug_image_iterations(
            task=task, metric=metric, variant=variant
        )
        self.assertEqual(res.min_iteration, 0)
        self.assertEqual(res.max_iteration, 0)
        res = self.api.events.get_debug_image_event(
            task=task, metric=metric, variant=variant
        )
        self.assertEqual(res.event, None)

        # test existing events
        iterations = 10
        unique_images = 4
        events = [
            self._create_task_event(
                task=task,
                iteration=n,
                metric=metric,
                variant=variant,
                url=f"{metric}_{variant}_{n % unique_images}",
            )
            for n in range(iterations)
        ]
        self.send_batch(events)
        res = self.api.events.get_debug_image_iterations(
            task=task, metric=metric, variant=variant
        )
        self.assertEqual(res.max_iteration, iterations-1)
        self.assertEqual(res.min_iteration, max(0, iterations - unique_images))

        # if iteration is not specified then return the event from the last one
        res = self.api.events.get_debug_image_event(
            task=task, metric=metric, variant=variant
        )
        self._assertEqualEvent(res.event, events[-1])

        # else from the specific iteration
        iteration = 8
        res = self.api.events.get_debug_image_event(
            task=task, metric=metric, variant=variant, iteration=iteration
        )
        self._assertEqualEvent(res.event, events[iteration])

    def _assertEqualEvent(self, ev1: dict, ev2: dict):
        self.assertEqual(ev1["iter"], ev2["iter"])
        self.assertEqual(ev1["url"], ev2["url"])

    def test_task_debug_images(self):
        task = self._temp_task()
        metric = "Metric1"
        variants = [("Variant1", 7), ("Variant2", 4)]
        iterations = 10

        # test empty
        res = self.api.events.debug_images(
            metrics=[{"task": task, "metric": metric}], iters=5,
        )
        self.assertFalse(res.metrics)

        # create events
        events = [
            self._create_task_event(
                task=task,
                iteration=n,
                metric=metric,
                variant=variant,
                url=f"{metric}_{variant}_{n % unique_images}",
            )
            for n in range(iterations)
            for (variant, unique_images) in variants
        ]
        self.send_batch(events)

        # init testing
        unique_images = [unique for (_, unique) in variants]
        scroll_id = None
        assert_debug_images = partial(
            self._assertDebugImages,
            task=task,
            metric=metric,
            max_iter=iterations - 1,
            unique_images=unique_images,
        )

        # test forward navigation
        for page in range(3):
            scroll_id = assert_debug_images(scroll_id=scroll_id, expected_page=page)

        # test backwards navigation
        scroll_id = assert_debug_images(
            scroll_id=scroll_id, expected_page=0, navigate_earlier=False
        )

        # beyond the latest iteration and back
        res = self.api.events.debug_images(
            metrics=[{"task": task, "metric": metric}],
            iters=5,
            scroll_id=scroll_id,
            navigate_earlier=False,
        )
        self.assertEqual(len(res["metrics"][0]["iterations"]), 0)
        assert_debug_images(scroll_id=scroll_id, expected_page=1)

        # refresh
        assert_debug_images(scroll_id=scroll_id, expected_page=0, refresh=True)

    def _assertDebugImages(
        self,
        task,
        metric,
        max_iter: int,
        unique_images: Sequence[int],
        scroll_id,
        expected_page: int,
        iters: int = 5,
        **extra_params,
    ):
        res = self.api.events.debug_images(
            metrics=[{"task": task, "metric": metric}],
            iters=iters,
            scroll_id=scroll_id,
            **extra_params,
        )
        data = res["metrics"][0]
        self.assertEqual(data["task"], task)
        self.assertEqual(data["metric"], metric)
        left_iterations = max(0, max(unique_images) - expected_page * iters)
        self.assertEqual(len(data["iterations"]), min(iters, left_iterations))
        for it in data["iterations"]:
            events_per_iter = sum(
                1 for unique in unique_images if unique > max_iter - it["iter"]
            )
            self.assertEqual(len(it["events"]), events_per_iter)
        return res.scroll_id

    def send_batch(self, events):
        _, data = self.api.send_batch("events.add_batch", events)
        return data
