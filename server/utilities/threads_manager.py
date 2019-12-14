from functools import wraps
from threading import Lock, Thread


class ThreadsManager:
    objects = {}
    lock = Lock()

    def __init__(self, name=None, **threads):
        super(ThreadsManager, self).__init__()
        self.name = name or self.__class__.name
        self.objects = {}
        self.lock = Lock()

        for thread_name, thread in threads.items():
            if issubclass(thread, Thread):
                thread = thread()
                thread.start()
            elif isinstance(thread, Thread):
                if not thread.is_alive():
                    thread.start()
            else:
                raise Exception(f"Expected thread or thread class ({thread_name}): {thread}")

            self.objects[thread_name] = thread

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
