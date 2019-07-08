from functools import wraps
from threading import Lock, Thread

import attr


@attr.s(auto_attribs=True)
class ThreadsManager:
    objects = {}
    lock = Lock()

    def register(self, thread_name, daemon=True):
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                with self.lock:
                    thread = self.objects.get(thread_name)
                    if not thread:
                        thread = Thread(
                            target=f, name=thread_name, args=args, kwargs=kwargs
                        )
                        thread.daemon = daemon
                        thread.start()
                        self.objects[thread_name] = thread
                    return thread.ident

            return wrapper

        return decorator
