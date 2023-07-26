from functools import wraps
from threading import Lock, Thread


class ThreadsManager:
    objects = {}
    lock = Lock()

    def __init__(self, name=None):
        self.name = name or self.__class__.__name__

    def register(self, thread_name, daemon=True):
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                with self.lock:
                    thread = self.objects.get(thread_name)
                    if not thread:
                        thread = Thread(
                            target=f, name=f"{self.name}_{thread_name}", args=args, kwargs=kwargs
                        )
                        thread.daemon = daemon
                        thread.start()
                        self.objects[thread_name] = thread
                    return thread.ident

            return wrapper

        return decorator

    def __getattr__(self, item):
        if item in self.objects:
            return self.objects[item]
        return self.__getattribute__(item)

    def __getitem__(self, item):
        if item in self.objects:
            return self.objects[item]
        raise KeyError(item)
