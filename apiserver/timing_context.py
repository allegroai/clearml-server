class TimingStats:
    @classmethod
    def aggregate(cls):
        return {}


class TimingContext:
    def __init__(self, *_, **__):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
