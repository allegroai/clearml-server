import time

from config import config

log = config.logger(__file__)

_stats = dict()


def stats():
    aggregate()
    return _stats


def clear():
    global _stats
    _stats = dict()


def get_component_total(comp):
    if comp not in _stats:
        return 0

    return _stats[comp].get("total")


# create a "total" node for each componenet
def aggregate():
    grand_total = 0
    for comp in _stats:
        total = 0
        for op in _stats[comp]:
            total += _stats[comp][op]
        _stats[comp]["total"] = total
        grand_total += total
    _stats["_all"] = dict(total=grand_total)


class TimingContext:
    def __init__(self, component, operation):
        self.component = component
        self.operation = operation
        _stats["_all"] = dict(total=0)
        if component not in _stats:
            _stats[component] = dict(total=0)

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        try:
            self.end = time.time()
            latency_ms = int((self.end - self.start) * 1000)
            if self.operation in _stats.get(self.component, {}):
                previous_latency = _stats[self.component][self.operation]
                new_latency = int((previous_latency + latency_ms) / 2)
            else:
                new_latency = latency_ms

            if self.component not in _stats:
                _stats[self.component] = dict(total=0)
            _stats[self.component][self.operation] = new_latency
        except Exception as ex:
            log.error("%s calculating latency: %s" % (type(ex).__name__, str(ex)))
