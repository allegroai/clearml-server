from functools import partial
from typing import Sequence, Mapping, Optional

from apiserver.es_factory import es_factory
from apiserver.tests.automated import TestService


class TestTaskPlots(TestService):
    def _temp_task(self, name="test task events"):
        task_input = dict(
            name=name, type="training", input=dict(mapping={}, view=dict(entries=[])),
        )
        return self.create_temp("tasks", **task_input)

    @staticmethod
    def _create_task_event(task, iteration, **kwargs):
        return {
            "worker": "test",
            "type": "plot",
            "task": task,
            "iter": iteration,
            "timestamp": kwargs.get("timestamp") or es_factory.get_timestamp_millis(),
            **kwargs,
        }

    def test_get_plot_sample(self):
        task = self._temp_task()
        metric = "Metric1"
        variant = "Variant1"

        # test empty
        res = self.api.events.get_plot_sample(
            task=task, metric=metric, variant=variant
        )
        self.assertEqual(res.min_iteration, None)
        self.assertEqual(res.max_iteration, None)
        self.assertEqual(res.event, None)

        # test existing events
        iterations = 10
        events = [
            self._create_task_event(
                task=task,
                iteration=n,
                metric=metric,
                variant=variant,
                plot_str=f"Test plot str {n}",
            )
            for n in range(iterations)
        ]
        self.send_batch(events)

        # if iteration is not specified then return the event from the last one
        res = self.api.events.get_plot_sample(
            task=task, metric=metric, variant=variant
        )
        self._assertEqualEvent(res.event, events[-1])
        self.assertEqual(res.max_iteration, iterations - 1)
        self.assertEqual(res.min_iteration, 0)
        self.assertTrue(res.scroll_id)

        # else from the specific iteration
        iteration = 8
        res = self.api.events.get_plot_sample(
            task=task,
            metric=metric,
            variant=variant,
            iteration=iteration,
            scroll_id=res.scroll_id,
        )
        self._assertEqualEvent(res.event, events[iteration])

    def test_next_plot_sample(self):
        task = self._temp_task()
        metric1 = "Metric1"
        variant1 = "Variant1"
        metric2 = "Metric2"
        variant2 = "Variant2"
        metrics = [(metric1,  variant1), (metric2, variant2)]
        # test existing events
        events = [
            self._create_task_event(
                task=task,
                iteration=n,
                metric=metric,
                variant=variant,
                plot_str=f"Test plot str {n}",
            )
            for n in range(2)
            for metric, variant in metrics
        ]
        self.send_batch(events)

        # single metric navigation
        # init scroll
        res = self.api.events.get_plot_sample(
            task=task, metric=metric1, variant=variant1
        )
        self._assertEqualEvent(res.event, events[-2])

        # navigate forwards
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id, navigate_earlier=False
        )
        self.assertEqual(res.event, None)

        # navigate backwards
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id
        )
        self._assertEqualEvent(res.event, events[-4])
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id
        )
        self._assertEqualEvent(res.event, None)

        # all metrics navigation
        # init scroll
        res = self.api.events.get_plot_sample(
            task=task, metric=metric1, variant=variant1, navigate_current_metric=False
        )
        self._assertEqualEvent(res.event, events[-2])

        # navigate forwards
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id, navigate_earlier=False
        )
        self._assertEqualEvent(res.event, events[-1])

        # navigate backwards
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id
        )
        self._assertEqualEvent(res.event, events[-2])
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id
        )
        self._assertEqualEvent(res.event, events[-3])

        # next_iteration
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id, next_iteration=True
        )
        self._assertEqualEvent(res.event, None)
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id, next_iteration=True, navigate_earlier=False
        )
        self._assertEqualEvent(res.event, events[-2])
        self.assertEqual(res.event.iter, 1)
        res = self.api.events.next_plot_sample(
            task=task, scroll_id=res.scroll_id, next_iteration=True, navigate_earlier=False
        )
        self._assertEqualEvent(res.event, None)

    def _assertEqualEvent(self, ev1: dict, ev2: Optional[dict]):
        if ev2 is None:
            self.assertIsNone(ev1)
            return
        self.assertIsNotNone(ev1)
        for field in ("iter", "timestamp", "metric", "variant", "plot_str", "task"):
            self.assertEqual(ev1[field], ev2[field])

    def test_task_plots(self):
        task = self._temp_task()

        # test empty
        res = self.api.events.plots(metrics=[{"task": task}], iters=5)
        self.assertFalse(res.metrics[0].iterations)
        res = self.api.events.plots(
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
                plot_str=f"Test plot str {metric}_{variant}",
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
                plot_str=f"Test plot str {metric}_{variant}_2",
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

    def _assertTaskMetrics(
        self,
        task: str,
        expected_metrics: Mapping[str, Sequence[str]],
        iterations,
        scroll_id: str = None,
        refresh=False,
    ) -> str:
        res = self.api.events.plots(
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

    def test_plots_navigation(self):
        task = self._temp_task()
        metric = "Metric1"
        variants = ["Variant1", "Variant2"]
        iterations = 10

        # test empty
        res = self.api.events.plots(
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
                plot_str=f"{metric}_{variant}_{n}",
            )
            for n in range(iterations)
            for variant in variants
        ]
        self.send_batch(events)

        # init testing
        scroll_id = None
        assert_plots = partial(
            self._assertPlots,
            task=task,
            metric=metric,
            iterations=iterations,
            variants=len(variants)
        )

        # test forward navigation
        for page in range(3):
            scroll_id = assert_plots(scroll_id=scroll_id, expected_page=page)

        # test backwards navigation
        scroll_id = assert_plots(
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
        assert_plots(scroll_id=scroll_id, expected_page=1)

        # refresh
        assert_plots(scroll_id=scroll_id, expected_page=0, refresh=True)

    def _assertPlots(
        self,
        task,
        metric,
        iterations: int,
        variants: int,
        scroll_id,
        expected_page: int,
        iters: int = 5,
        **extra_params,
    ) -> str:
        res = self.api.events.plots(
            metrics=[{"task": task, "metric": metric}],
            iters=iters,
            scroll_id=scroll_id,
            **extra_params,
        )
        data = res["metrics"][0]
        self.assertEqual(data["task"], task)
        left_iterations = max(0, iterations - expected_page * iters)
        self.assertEqual(len(data["iterations"]), min(iters, left_iterations))
        for it in data["iterations"]:
            self.assertEqual(len(it["events"]), variants)
        return res.scroll_id

    def send_batch(self, events):
        _, data = self.api.send_batch("events.add_batch", events)
        return data
