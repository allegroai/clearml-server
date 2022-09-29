from datetime import datetime
import operator
from threading import Lock
from time import sleep

import attr
import psutil

from apiserver.utilities.threads_manager import ThreadsManager


stat_threads = ThreadsManager("Statistics")


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

    @classmethod
    def get_current_sample(cls) -> "Sample":
        return cls(
            cpu_usage=psutil.cpu_percent(),
            mem_used_gb=psutil.virtual_memory().used / (1024 ** 3),
            mem_free_gb=psutil.virtual_memory().free / (1024 ** 3),
        )


class ResourceMonitor:
    class Accumulator:
        def __init__(self):
            sample = Sample.get_current_sample()
            self.avg = sample
            self.min = sample
            self.max = sample
            self.time = datetime.utcnow()
            self.count = 1

        def add_sample(self, sample: Sample):
            self.min = self.min.min(sample)
            self.max = self.max.max(sample)
            self.avg = self.avg.avg(sample, self.count)
            self.count += 1

    sample_interval_sec = 5
    _lock = Lock()
    accumulator = Accumulator()

    @classmethod
    @stat_threads.register("resource_monitor", daemon=True)
    def start(cls):
        while True:
            sleep(cls.sample_interval_sec)
            sample = Sample.get_current_sample()
            with cls._lock:
                cls.accumulator.add_sample(sample)

    @classmethod
    def get_stats(cls) -> dict:
        """ Returns current resource statistics and clears internal resource statistics """
        with cls._lock:
            min_ = attr.asdict(cls.accumulator.min)
            max_ = attr.asdict(cls.accumulator.max)
            avg = attr.asdict(cls.accumulator.avg)
            interval = datetime.utcnow() - cls.accumulator.time
            cls.accumulator = cls.Accumulator()

        return {
            "interval_sec": interval.total_seconds(),
            "num_cores": psutil.cpu_count(),
            **{
                k: {"min": v, "max": max_[k], "avg": avg[k]}
                for k, v in min_.items()
            }
        }
