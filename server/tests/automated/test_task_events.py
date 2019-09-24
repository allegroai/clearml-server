"""
Comprehensive test of all(?) use cases of datasets and frames
"""
import json
import unittest
from statistics import mean

import es_factory
from config import config
from tests.automated import TestService

log = config.logger(__file__)


class TestTaskEvents(TestService):
    def setUp(self, version="1.7"):
        super().setUp(version=version)

        self.created_tasks = []

        self.task = dict(
            name="test task events",
            type="training",
            input=dict(mapping={}, view=dict(entries=[])),
        )
        res, self.task_id = self.api.send("tasks.create", self.task, extract="id")
        assert res.meta.result_code == 200
        self.created_tasks.append(self.task_id)

    def tearDown(self):
        log.info("Cleanup...")
        for task_id in self.created_tasks:
            try:
                self.api.send("tasks.delete", dict(task=task_id, force=True))
            except Exception as ex:
                log.exception(ex)

    def create_task_event(self, type, iteration):
        return {
            "worker": "test",
            "type": type,
            "task": self.task_id,
            "iter": iteration,
            "timestamp": es_factory.get_timestamp_millis()
        }

    def copy_and_update(self, src_obj, new_data):
        obj = src_obj.copy()
        obj.update(new_data)
        return obj

    def test_task_logs(self):
        events = []
        for iter in range(10):
            log_event = self.create_task_event("log", iteration=iter)
            events.append(
                self.copy_and_update(
                    log_event,
                    {"msg": "This is a log message from test task iter " + str(iter)},
                )
            )
            # sleep so timestamp is not the same
            import time

            time.sleep(0.01)
        self.send_batch(events)

        data = self.api.events.get_task_log(task=self.task_id)
        assert len(data["events"]) == 10

        self.api.tasks.reset(task=self.task_id)
        data = self.api.events.get_task_log(task=self.task_id)
        assert len(data["events"]) == 0

    def test_task_metric_value_intervals_keys(self):
        metric = "Metric1"
        variant = "Variant1"
        iter_count = 100
        events = [
            {
                **self.create_task_event("training_stats_scalar", iteration),
                "metric": metric,
                "variant": variant,
                "value": iteration,
            }
            for iteration in range(iter_count)
        ]
        self.send_batch(events)
        for key in None, "iter", "timestamp", "iso_time":
            with self.subTest(key=key):
                data = self.api.events.scalar_metrics_iter_histogram(task=self.task_id, key=key)
                self.assertIn(metric, data)
                self.assertIn(variant, data[metric])
                self.assertIn("x", data[metric][variant])
                self.assertIn("y", data[metric][variant])

    def test_task_metric_value_intervals(self):
        metric = "Metric1"
        variant = "Variant1"
        iter_count = 100
        events = [
            {
                **self.create_task_event("training_stats_scalar", iteration),
                "metric": metric,
                "variant": variant,
                "value": iteration,
            }
            for iteration in range(iter_count)
        ]
        self.send_batch(events)

        data = self.api.events.scalar_metrics_iter_histogram(task=self.task_id)
        self._assert_metrics_histogram(data[metric][variant], iter_count, 100)

        data = self.api.events.scalar_metrics_iter_histogram(task=self.task_id, samples=100)
        self._assert_metrics_histogram(data[metric][variant], iter_count, 100)

        data = self.api.events.scalar_metrics_iter_histogram(task=self.task_id, samples=10)
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
        event = self.create_task_event("plot", 0)
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

        event = self.create_task_event("plot", 100)
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

        data = self.api.events.get_task_plots(task=self.task_id)
        assert len(data["plots"]) == 2

        self.api.tasks.reset(task=self.task_id)
        data = self.api.events.get_task_plots(task=self.task_id)
        assert len(data["plots"]) == 0

    def send_batch(self, events):
        self.api.send_batch("events.add_batch", events)

    def send(self, event):
        self.api.send("events.add", event)


if __name__ == "__main__":
    unittest.main()
