from functools import partial
from typing import Sequence, Mapping

from apiserver.es_factory import es_factory
from apiserver.tests.automated import TestService


class TestTaskDebugImages(TestService):
    def setUp(self, version="2.12"):
        super().setUp(version=version)

    def _temp_task(self, name="test task events"):
        task_input = dict(
            name=name, type="training"
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

    def test_get_debug_image_sample(self):
        task = self._temp_task()
        metric = "Metric1"
        variant = "Variant1"

        # test empty
        res = self.api.events.get_debug_image_sample(
            task=task, metric=metric, variant=variant
        )
        self.assertEqual(res.min_iteration, None)
        self.assertEqual(res.max_iteration, None)
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

        # if iteration is not specified then return the event from the last one
        res = self.api.events.get_debug_image_sample(
            task=task, metric=metric, variant=variant
        )
        self._assertEqualEvent(res.event, events[-1])
        self.assertEqual(res.max_iteration, iterations - 1)
        self.assertEqual(res.min_iteration, max(0, iterations - unique_images))
        self.assertTrue(res.scroll_id)

        # else from the specific iteration
        iteration = 8
        res = self.api.events.get_debug_image_sample(
            task=task,
            metric=metric,
            variant=variant,
            iteration=iteration,
            scroll_id=res.scroll_id,
        )
        self._assertEqualEvent(res.event, events[iteration])

    def test_next_debug_image_sample(self):
        task = self._temp_task()
        metric = "Metric1"
        variant1 = "Variant1"
        variant2 = "Variant2"

        # test existing events
        events = [
            self._create_task_event(
                task=task,
                iteration=n,
                metric=metric,
                variant=v,
                url=f"{metric}_{v}_{n}",
            )
            for n in range(2)
            for v in (variant1, variant2)
        ]
        self.send_batch(events)

        # init scroll
        res = self.api.events.get_debug_image_sample(
            task=task, metric=metric, variant=variant1
        )
        self._assertEqualEvent(res.event, events[-2])

        # navigate forwards
        res = self.api.events.next_debug_image_sample(
            task=task, scroll_id=res.scroll_id, navigate_earlier=False
        )
        self._assertEqualEvent(res.event, events[-1])
        res = self.api.events.next_debug_image_sample(
            task=task, scroll_id=res.scroll_id, navigate_earlier=False
        )
        self.assertEqual(res.event, None)

        # navigate backwards
        for i in range(3):
            res = self.api.events.next_debug_image_sample(
                task=task, scroll_id=res.scroll_id
            )
            self._assertEqualEvent(res.event, events[-2 - i])
        res = self.api.events.next_debug_image_sample(
            task=task, scroll_id=res.scroll_id
        )
        self.assertEqual(res.event, None)

    def _assertEqualEvent(self, ev1: dict, ev2: dict):
        self.assertEqual(ev1["iter"], ev2["iter"])
        self.assertEqual(ev1["url"], ev2["url"])

    def test_task_debug_images(self):
        task = self._temp_task()

        # test empty
        res = self.api.events.debug_images(metrics=[{"task": task}], iters=5)
        self.assertFalse(res.metrics[0].iterations)
        res = self.api.events.debug_images(
            metrics=[{"task": task}], iters=5, scroll_id=res.scroll_id, refresh=True
        )
        self.assertFalse(res.metrics[0].iterations)

        # test not empty
        metrics = {
            "Metric1": ["Variant1", "Variant2"],
            "Metric2": ["Variant3", "Variant4"],
        }
        events = [
            self._create_task_event(
                task=task,
                iteration=1,
                metric=metric,
                variant=variant,
                url=f"{metric}_{variant}_{1}",
            )
            for metric, variants in metrics.items()
            for variant in variants
        ]
        self.send_batch(events)
        scroll_id = self._assertTaskMetrics(
            task=task, expected_metrics=metrics, iterations=1
        )

        # test refresh
        update = {
            "Metric2": ["Variant3", "Variant4", "Variant5"],
            "Metric3": ["VariantA", "VariantB"],
        }
        events = [
            self._create_task_event(
                task=task,
                iteration=2,
                metric=metric,
                variant=variant,
                url=f"{metric}_{variant}_{2}",
            )
            for metric, variants in update.items()
            for variant in variants
        ]
        self.send_batch(events)
        # without refresh the metric states are not updated
        scroll_id = self._assertTaskMetrics(
            task=task, expected_metrics=metrics, iterations=0, scroll_id=scroll_id
        )

        # with refresh there are new metrics and existing ones are updated
        self._assertTaskMetrics(
            task=task,
            expected_metrics=update,
            iterations=1,
            scroll_id=scroll_id,
            refresh=True,
        )

        pass

    def _assertTaskMetrics(
        self,
        task: str,
        expected_metrics: Mapping[str, Sequence[str]],
        iterations,
        scroll_id: str = None,
        refresh=False,
    ) -> str:
        res = self.api.events.debug_images(
            metrics=[{"task": task}], iters=1, scroll_id=scroll_id, refresh=refresh
        )
        if not iterations:
            self.assertTrue(all(m.iterations == [] for m in res.metrics))
            return res.scroll_id

        expected_variants = set((m, var) for m, vars_ in expected_metrics.items() for var in vars_)
        for metric_data in res.metrics:
            self.assertEqual(len(metric_data.iterations), iterations)
            for it_data in metric_data.iterations:
                self.assertEqual(
                    set((e.metric, e.variant) for e in it_data.events), expected_variants
                )

        return res.scroll_id

    def test_get_debug_images_navigation(self):
        task = self._temp_task()
        metric = "Metric1"
        variants = [("Variant1", 7), ("Variant2", 4)]
        iterations = 10

        # test empty
        res = self.api.events.debug_images(
            metrics=[{"task": task, "metric": metric}], iters=5,
        )
        self.assertFalse(res.metrics[0].iterations)

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
    ) -> str:
        res = self.api.events.debug_images(
            metrics=[{"task": task, "metric": metric}],
            iters=iters,
            scroll_id=scroll_id,
            **extra_params,
        )
        data = res["metrics"][0]
        self.assertEqual(data["task"], task)
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
