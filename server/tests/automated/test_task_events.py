"""
Comprehensive test of all(?) use cases of datasets and frames
"""
import json
import operator
import unittest
from functools import partial
from statistics import mean
from typing import Sequence

import es_factory
from apierrors.errors.bad_request import EventsNotAdded
from tests.automated import TestService


class TestTaskEvents(TestService):
    def setUp(self, version="2.7"):
        super().setUp(version=version)

    def _temp_task(self, name="test task events"):
        task_input = dict(
            name=name, type="training", input=dict(mapping={}, view=dict(entries=[])),
        )
        return self.create_temp("tasks", **task_input)

    @staticmethod
    def _create_task_event(type_, task, iteration, **kwargs):
        return {
            "worker": "test",
            "type": type_,
            "task": task,
            "iter": iteration,
            "timestamp": kwargs.get("timestamp") or es_factory.get_timestamp_millis(),
            **kwargs,
        }

    def test_task_metrics(self):
        tasks = {
            self._temp_task(): {
                "Metric1": ["training_debug_image"],
                "Metric2": ["training_debug_image", "log"],
            },
            self._temp_task(): {"Metric3": ["training_debug_image"]},
        }
        events = [
            self._create_task_event(
                event_type,
                task=task,
                iteration=1,
                metric=metric,
                variant="Test variant",
            )
            for task, metrics in tasks.items()
            for metric, event_types in metrics.items()
            for event_type in event_types
        ]
        self.send_batch(events)
        self._assert_task_metrics(tasks, "training_debug_image")
        self._assert_task_metrics(tasks, "log")
        self._assert_task_metrics(tasks, "training_stats_scalar")

    def _assert_task_metrics(self, tasks: dict, event_type: str):
        res = self.api.events.get_task_metrics(tasks=list(tasks), event_type=event_type)
        for task, metrics in tasks.items():
            res_metrics = next(
                (tm.metrics for tm in res.metrics if tm.task == task), ()
            )
            self.assertEqual(
                set(res_metrics),
                set(
                    metric for metric, events in metrics.items() if event_type in events
                ),
            )

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
                "training_debug_image",
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

    def test_error_events(self):
        task = self._temp_task()
        events = [
            self._create_task_event("unknown type", task, iteration=1),
            self._create_task_event("training_debug_image", task=None, iteration=1),
            self._create_task_event(
                "training_debug_image", task="Invalid task", iteration=1
            ),
        ]
        # failure if no events added
        with self.api.raises(EventsNotAdded):
            self.send_batch(events)

        events.append(
            self._create_task_event("training_debug_image", task=task, iteration=1)
        )
        # success if at least one event added
        res = self.send_batch(events)
        self.assertEqual(res["added"], 1)
        self.assertEqual(res["errors"], 3)
        self.assertEqual(len(res["errors_info"]), 3)
        res = self.api.events.get_task_events(task=task)
        self.assertEqual(len(res.events), 1)

    def test_task_logs(self):
        # this test will fail until the new api is uncommented
        task = self._temp_task()
        timestamp = es_factory.get_timestamp_millis()
        events = [
            self._create_task_event(
                "log",
                task=task,
                iteration=iter_,
                timestamp=timestamp + iter_ * 1000,
                msg=f"This is a log message from test task iter {iter_}",
            )
            for iter_ in range(10)
        ]
        self.send_batch(events)

        # test forward navigation
        scroll_id = None
        for page in range(3):
            scroll_id = self._assert_log_events(
                task=task, scroll_id=scroll_id, expected_page=page
            )

        # test backwards navigation
        scroll_id = self._assert_log_events(
            task=task, scroll_id=scroll_id, navigate_earlier=False
        )

        # refresh
        self._assert_log_events(task=task, scroll_id=scroll_id)
        self._assert_log_events(task=task, scroll_id=scroll_id, refresh=True)

    def _assert_log_events(
        self,
        task,
        scroll_id,
        batch_size: int = 5,
        expected_total: int = 10,
        expected_page: int = 0,
        **extra_params,
    ):
        res = self.api.events.get_task_log(
            task=task, batch_size=batch_size, scroll_id=scroll_id, **extra_params,
        )
        self.assertEqual(res.total, expected_total)
        expected_events = max(
            0, batch_size - max(0, (expected_page + 1) * batch_size - expected_total)
        )
        self.assertEqual(res.returned, expected_events)
        self.assertEqual(len(res.events), expected_events)
        unique_events = len({ev.iter for ev in res.events})
        self.assertEqual(len(res.events), unique_events)
        if res.events:
            cmp_operator = operator.ge
            if not extra_params.get("navigate_earlier", True):
                cmp_operator = operator.le
            self.assertTrue(
                all(
                    cmp_operator(first.timestamp, second.timestamp)
                    for first, second in zip(res.events, res.events[1:])
                )
            )
        return res.scroll_id

    def test_task_metric_value_intervals_keys(self):
        metric = "Metric1"
        variant = "Variant1"
        iter_count = 100
        task = self._temp_task()
        events = [
            {
                **self._create_task_event("training_stats_scalar", task, iteration),
                "metric": metric,
                "variant": variant,
                "value": iteration,
            }
            for iteration in range(iter_count)
        ]
        self.send_batch(events)
        for key in None, "iter", "timestamp", "iso_time":
            with self.subTest(key=key):
                data = self.api.events.scalar_metrics_iter_histogram(task=task, key=key)
                self.assertIn(metric, data)
                self.assertIn(variant, data[metric])
                self.assertIn("x", data[metric][variant])
                self.assertIn("y", data[metric][variant])

    def test_multitask_events_many_metrics(self):
        tasks = [
            self._temp_task(name="test events1"),
            self._temp_task(name="test events2"),
        ]
        iter_count = 10
        metrics_count = 10
        variants_count = 10
        events = [
            {
                **self._create_task_event("training_stats_scalar", task, iteration),
                "metric": f"Metric{metric_idx}",
                "variant": f"Variant{variant_idx}",
                "value": iteration,
            }
            for iteration in range(iter_count)
            for task in tasks
            for metric_idx in range(metrics_count)
            for variant_idx in range(variants_count)
        ]
        self.send_batch(events)
        data = self.api.events.multi_task_scalar_metrics_iter_histogram(tasks=tasks)
        self._assert_metrics_and_variants(
            data.metrics,
            metrics=metrics_count,
            variants=variants_count,
            tasks=tasks,
            iterations=iter_count,
        )

    def _assert_metrics_and_variants(
        self, data: dict, metrics: int, variants: int, tasks: Sequence, iterations: int
    ):
        self.assertEqual(len(data), metrics)
        for m in range(metrics):
            metric_data = data[f"Metric{m}"]
            self.assertEqual(len(metric_data), variants)
            for v in range(variants):
                variant_data = metric_data[f"Variant{v}"]
                self.assertEqual(len(variant_data), len(tasks))
                for t in tasks:
                    task_data = variant_data[t]
                    self.assertEqual(len(task_data["x"]), iterations)
                    self.assertEqual(len(task_data["y"]), iterations)

    def test_task_metric_value_intervals(self):
        metric = "Metric1"
        variant = "Variant1"
        iter_count = 100
        task = self._temp_task()
        events = [
            {
                **self._create_task_event("training_stats_scalar", task, iteration),
                "metric": metric,
                "variant": variant,
                "value": iteration,
            }
            for iteration in range(iter_count)
        ]
        self.send_batch(events)

        data = self.api.events.scalar_metrics_iter_histogram(task=task)
        self._assert_metrics_histogram(data[metric][variant], iter_count, 100)

        data = self.api.events.scalar_metrics_iter_histogram(task=task, samples=100)
        self._assert_metrics_histogram(data[metric][variant], iter_count, 100)

        data = self.api.events.scalar_metrics_iter_histogram(task=task, samples=10)
        self._assert_metrics_histogram(data[metric][variant], iter_count, 10)

    def _assert_metrics_histogram(self, data, iters, samples):
        interval = iters // samples
        self.assertEqual(len(data["x"]), samples)
        self.assertEqual(len(data["y"]), samples)
        for curr in range(samples):
            self.assertEqual(data["x"][curr], curr * interval)
            self.assertEqual(
                data["y"][curr],
                mean(v for v in range(curr * interval, (curr + 1) * interval)),
            )

    def test_task_plots(self):
        task = self._temp_task()
        event = self._create_task_event("plot", task, 0)
        event["metric"] = "roc"
        event.update(
            {
                "plot_str": json.dumps(
                    {
                        "data": [
                            {
                                "x": [0, 1, 2, 3, 4, 5, 6, 7, 8],
                                "y": [0, 1, 2, 3, 4, 5, 6, 7, 8],
                                "text": [
                                    "Th=0.1",
                                    "Th=0.2",
                                    "Th=0.3",
                                    "Th=0.4",
                                    "Th=0.5",
                                    "Th=0.6",
                                    "Th=0.7",
                                    "Th=0.8",
                                ],
                                "name": "class1",
                            },
                            {
                                "x": [0, 1, 2, 3, 4, 5, 6, 7, 8],
                                "y": [2.0, 3.0, 5.0, 8.2, 6.4, 7.5, 9.2, 8.1, 10.0],
                                "text": [
                                    "Th=0.1",
                                    "Th=0.2",
                                    "Th=0.3",
                                    "Th=0.4",
                                    "Th=0.5",
                                    "Th=0.6",
                                    "Th=0.7",
                                    "Th=0.8",
                                ],
                                "name": "class2",
                            },
                        ],
                        "layout": {
                            "title": "ROC for iter 0",
                            "xaxis": {"title": "my x axis"},
                            "yaxis": {"title": "my y axis"},
                        },
                    }
                )
            }
        )
        self.send(event)

        event = self._create_task_event("plot", task, 100)
        event["metric"] = "confusion"
        event.update(
            {
                "plot_str": json.dumps(
                    {
                        "data": [
                            {
                                "y": [
                                    "lying",
                                    "sitting",
                                    "standing",
                                    "people",
                                    "backgroun",
                                ],
                                "x": [
                                    "lying",
                                    "sitting",
                                    "standing",
                                    "people",
                                    "backgroun",
                                ],
                                "z": [
                                    [758, 163, 0, 0, 23],
                                    [63, 858, 3, 0, 0],
                                    [0, 50, 188, 21, 35],
                                    [0, 22, 8, 40, 4],
                                    [12, 91, 26, 29, 368],
                                ],
                                "type": "heatmap",
                            }
                        ],
                        "layout": {
                            "title": "Confusion Matrix for iter 100",
                            "xaxis": {"title": "Predicted value"},
                            "yaxis": {"title": "Real value"},
                        },
                    }
                )
            }
        )
        self.send(event)

        data = self.api.events.get_task_plots(task=task)
        assert len(data["plots"]) == 2

        self.api.tasks.reset(task=task)
        data = self.api.events.get_task_plots(task=task)
        assert len(data["plots"]) == 0

    def send_batch(self, events):
        _, data = self.api.send_batch("events.add_batch", events)
        return data

    def send(self, event):
        self.api.send("events.add", event)


if __name__ == "__main__":
    unittest.main()
