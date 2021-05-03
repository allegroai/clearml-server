import os
import time
from contextlib import contextmanager
from time import sleep

from apiserver.redis_manager import redman

_redis = redman.connection("apiserver")


@contextmanager
def distributed_lock(name: str, timeout: int, max_wait: int = 0):
    """
    Context manager that acquires a distributed lock on enter
    and releases it on exit. The has a ttl equal to timeout seconds
    If the lock can not be acquired for wait seconds (defaults to timeout * 2)
    then the exception is thrown
    """
    lock_name = f"dist_lock_{name}"
    start = time.time()
    max_wait = max_wait or timeout * 2
    pid = os.getpid()
    while _redis.set(lock_name, value=pid, ex=timeout, nx=True) is None:
        sleep(1)
        if time.time() - start > max_wait:
            holder = _redis.get(lock_name)
            raise Exception(f"Could not acquire {name} lock for {max_wait} seconds. The lock is hold by {holder}")
    try:
        yield
    finally:
        _redis.delete(lock_name)
