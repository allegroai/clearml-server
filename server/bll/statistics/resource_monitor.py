from datetime import datetime
import operator
from threading import Thread, Lock
from time import sleep

import attr
import psutil


class ResourceMonitor(Thread):
    @attr.s(auto_attribs=True)
    class Sample:
        cpu_usage: float = 0.0
        mem_used_gb: float = 0
        mem_free_gb: float = 0

        @classmethod
        def _apply(cls, op, *samples):
            return cls(
                **{
                    field: op(*(getattr(sample, field) for sample in samples))
                    for field in attr.fields_dict(cls)
                }
            )

        def min(self, sample):
            return self._apply(min, self, sample)

        def max(self, sample):
            return self._apply(max, self, sample)

        def avg(self, sample, count):
            res = self._apply(lambda x: x * count, self)
            res = self._apply(operator.add, res, sample)
            res = self._apply(lambda x: x / (count + 1), res)
            return res

    def __init__(self, sample_interval_sec=5):
        super(ResourceMonitor, self).__init__(daemon=True)
        self.sample_interval_sec = sample_interval_sec
        self._lock = Lock()
        self._clear()

    def _clear(self):
        sample = self._get_sample()
        self._avg = sample
        self._min = sample
        self._max = sample
        self._clear_time = datetime.utcnow()
        self._count = 1

    @classmethod
    def _get_sample(cls) -> Sample:
        return cls.Sample(
            cpu_usage=psutil.cpu_percent(),
            mem_used_gb=psutil.virtual_memory().used / (1024 ** 3),
            mem_free_gb=psutil.virtual_memory().free / (1024 ** 3),
        )

    def run(self):
        while True:
            sleep(self.sample_interval_sec)

            sample = self._get_sample()

            with self._lock:
                self._min = self._min.min(sample)
                self._max = self._max.max(sample)
                self._avg = self._avg.avg(sample, self._count)
                self._count += 1

    def get_stats(self) -> dict:
        """ Returns current resource statistics and clears internal resource statistics """
        with self._lock:
            min_ = attr.asdict(self._min)
            max_ = attr.asdict(self._max)
            avg = attr.asdict(self._avg)
            interval = datetime.utcnow() - self._clear_time
            self._clear()

        return {
            "interval_sec": interval.total_seconds(),
            "num_cores": psutil.cpu_count(),
            **{
                k: {"min": v, "max": max_[k], "avg": avg[k]}
                for k, v in min_.items()
            }
        }
