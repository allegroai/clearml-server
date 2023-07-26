import time
from uuid import uuid4
from datetime import timedelta
from operator import attrgetter
from typing import Sequence

from apiserver.apierrors.errors import bad_request
from apiserver.tests.automated import TestService, utc_now_tz_aware
from apiserver.config_repo import config

log = config.logger(__file__)


class TestWorkersService(TestService):
    def _check_exists(self, worker: str, exists: bool = True, tags: list = None):
        workers = self.api.workers.get_all(last_seen=100, tags=tags).workers
        found = any(w for w in workers if w.id == worker)
        assert exists == found

    def test_workers_register(self):
        test_worker = f"test_{uuid4().hex}"
        self._check_exists(test_worker, False)

        self.api.workers.register(worker=test_worker)
        self._check_exists(test_worker)

        self.api.workers.unregister(worker=test_worker)
        self._check_exists(test_worker, False)

    def test_get_count(self):
        test_workers = [f"test_{uuid4().hex}" for _ in range(2)]
        system_tag = f"tag_{uuid4().hex}"
        for w in test_workers:
            self.api.workers.register(worker=w, system_tags=[system_tag])
        # total workers count include the new ones
        count = self.api.workers.get_count().count
        self.assertGreater(count, len(test_workers))
        # filter by system tag and last seen
        count = self.api.workers.get_count(system_tags=[system_tag], last_seen=4).count
        self.assertEqual(count, len(test_workers))
        time.sleep(5)
        # workers not seen recently
        count = self.api.workers.get_count(system_tags=[system_tag], last_seen=4).count
        self.assertEqual(count, 0)
        # but still visible without the last seen filter
        count = self.api.workers.get_count(system_tags=[system_tag]).count
        self.assertEqual(count, len(test_workers))

    def test_workers_timeout(self):
        test_worker = f"test_{uuid4().hex}"
        self._check_exists(test_worker, False)

        self.api.workers.register(worker=test_worker, timeout=3)
        self._check_exists(test_worker)

        time.sleep(5)
        self._check_exists(test_worker, False)

    def test_system_tags(self):
        test_worker = f"test_{uuid4().hex}"
        tag = uuid4().hex
        system_tag = uuid4().hex
        self.api.workers.register(
            worker=test_worker, tags=[tag], system_tags=[system_tag], timeout=5
        )

        # system_tags support
        worker = self.api.workers.get_all(tags=[tag], system_tags=[system_tag]).workers[
            0
        ]
        self.assertEqual(worker.id, test_worker)
        self.assertEqual(worker.tags, [tag])
        self.assertEqual(worker.system_tags, [system_tag])

        workers = self.api.workers.get_all(tags=[tag], system_tags=[f"-{system_tag}"]).workers
        self.assertFalse(workers)

    def test_filters(self):
        test_worker = f"test_{uuid4().hex}"
        self.api.workers.register(worker=test_worker, tags=["application"], timeout=3)
        self._check_exists(test_worker)
        self._check_exists(test_worker, tags=["application", "test"])
        self._check_exists(test_worker, False, tags=["test"])
        self._check_exists(test_worker, False, tags=["-application"])

    def _simulate_workers(self) -> Sequence[str]:
        """
        Two workers writing the same metrics. One for 4 seconds. Another one for 2
        The first worker reports a task
        :return: worker ids
        """

        task_id = self._create_running_task(task_name="task-1")

        workers = [f"test_{uuid4().hex}", f"test_{uuid4().hex}"]
        workers_stats = [
            (
                dict(cpu_usage=[10, 20], memory_used=50),
                dict(cpu_usage=[5], memory_used=30),
            )
        ] * 4
        workers_activity = [
            (workers[0], workers[1]),
            (workers[0], workers[1]),
            (workers[0],),
            (workers[0],),
        ]
        for ws, stats in zip(workers_activity, workers_stats):
            for w, s in zip(ws, stats):
                data = dict(
                    worker=w,
                    timestamp=int(utc_now_tz_aware().timestamp() * 1000),
                    machine_stats=s,
                )
                if w == workers[0]:
                    data["task"] = task_id
                self.api.workers.status_report(**data)
            time.sleep(1)

        res = self.api.workers.get_all(last_seen=100)
        return [w.key for w in res.workers]

    def _create_running_task(self, task_name):
        task_input = dict(
            name=task_name, type="testing"
        )

        task_id = self.create_temp("tasks", **task_input)

        self.api.tasks.started(task=task_id)
        return task_id

    def test_get_keys(self):
        workers = self._simulate_workers()
        res = self.api.workers.get_metric_keys(worker_ids=workers)
        assert {"cpu", "memory"} == set(c.name for c in res["categories"])
        assert all(
            c.metric_keys == ["cpu_usage"] for c in res["categories"] if c.name == "cpu"
        )
        assert all(
            c.metric_keys == ["memory_used"]
            for c in res["categories"]
            if c.name == "memory"
        )

        with self.api.raises(bad_request.WorkerStatsNotFound):
            self.api.workers.get_metric_keys(worker_ids=["Non existing worker id"])

    def test_get_stats(self):
        workers = self._simulate_workers()

        to_date = utc_now_tz_aware() + timedelta(seconds=10)
        from_date = to_date - timedelta(days=1)

        # no variants
        res = self.api.workers.get_stats(
            items=[
                dict(key="cpu_usage", aggregation="avg"),
                dict(key="cpu_usage", aggregation="max"),
                dict(key="memory_used", aggregation="max"),
                dict(key="memory_used", aggregation="min"),
            ],
            from_date=from_date.timestamp(),
            to_date=to_date.timestamp(),
            # split_by_variant=True,
            interval=1,
            worker_ids=workers,
        )
        self.assertWorkersInStats(workers, res["workers"])
        assert all(
            {"cpu_usage", "memory_used"}
            == set(map(attrgetter("metric"), worker["metrics"]))
            for worker in res["workers"]
        )

        def _check_dates_and_stats(metric, stats, worker_id) -> bool:
            return set(
                map(attrgetter("aggregation"), metric["stats"])
            ) == stats and len(metric["dates"]) == (4 if worker_id == workers[0] else 2)

        assert all(
            _check_dates_and_stats(metric, metric_stats, worker["worker"])
            for worker in res["workers"]
            for metric, metric_stats in zip(
                worker["metrics"], ({"avg", "max"}, {"max", "min"})
            )
        )

        # split by variants
        res = self.api.workers.get_stats(
            items=[dict(key="cpu_usage", aggregation="avg")],
            from_date=from_date.timestamp(),
            to_date=to_date.timestamp(),
            split_by_variant=True,
            interval=1,
            worker_ids=workers,
        )
        self.assertWorkersInStats(workers, res["workers"])

        def _check_metric_and_variants(worker):
            return (
                all(
                    _check_dates_and_stats(metric, {"avg"}, worker["worker"])
                    for metric in worker["metrics"]
                )
                and set(map(attrgetter("variant"), worker["metrics"])) == {"0", "1"}
                if worker["worker"] == workers[0]
                else {"0"}
            )

        assert all(_check_metric_and_variants(worker) for worker in res["workers"])

        res = self.api.workers.get_stats(
            items=[dict(key="cpu_usage", aggregation="avg")],
            from_date=from_date.timestamp(),
            to_date=to_date.timestamp(),
            interval=1,
            worker_ids=["Non existing worker id"],
        )
        assert not res["workers"]

    @staticmethod
    def assertWorkersInStats(workers: Sequence[str], stats: dict):
        assert set(workers) == set(map(attrgetter("worker"), stats))

    def test_get_activity_report(self):
        # test no workers data
        # run on an empty es db since we have no way
        # to pass non existing workers to this api
        # res = self.api.workers.get_activity_report(
        #     from_timestamp=from_timestamp.timestamp(),
        #     to_timestamp=to_timestamp.timestamp(),
        #     interval=20,
        # )

        self._simulate_workers()

        to_date = utc_now_tz_aware() + timedelta(seconds=10)
        from_date = to_date - timedelta(minutes=1)

        # no variants
        res = self.api.workers.get_activity_report(
            from_date=from_date.timestamp(), to_date=to_date.timestamp(), interval=20
        )
        self.assertWorkerSeries(res["total"], 2)
        self.assertWorkerSeries(res["active"], 1)
        self.assertTotalSeriesGreaterThenActive(res["total"], res["active"])

    @staticmethod
    def assertTotalSeriesGreaterThenActive(total_data: dict, active_data: dict):
        assert total_data["dates"][-1] == active_data["dates"][-1]
        assert total_data["counts"][-1] > active_data["counts"][-1]

    @staticmethod
    def assertWorkerSeries(series_data: dict, min_count: int):
        assert len(series_data["dates"]) == len(series_data["counts"])
        # check the last 20s aggregation
        # there may be more workers that we created since we are not filtering by test workers here
        assert series_data["counts"][-1] >= min_count
