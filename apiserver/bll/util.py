import functools
import itertools
from concurrent.futures.thread import ThreadPoolExecutor
from typing import (
    Optional,
    Callable,
    Iterable,
    Tuple,
    Sequence,
    TypeVar,
)

from boltons import iterutils

from apiserver.apierrors import APIError
from apiserver.database.model.settings import Settings


@functools.lru_cache()
def get_server_uuid() -> Optional[str]:
    return Settings.get_by_key("server.uuid")


def parallel_chunked_decorator(func: Callable = None, chunk_size: int = 100):
    """
    Decorates a method for parallel chunked execution. The method should have
    one positional parameter (that is used for breaking into chunks)
    and arbitrary number of keyword params. The return value should be iterable
    The results are concatenated in the same order as the passed params
    """
    if func is None:
        return functools.partial(parallel_chunked_decorator, chunk_size=chunk_size)

    @functools.wraps(func)
    def wrapper(self, iterable: Iterable, **kwargs):
        assert iterutils.is_collection(
            iterable
        ), "The positional parameter should be an iterable for breaking into chunks"

        func_with_params = functools.partial(func, self, **kwargs)
        with ThreadPoolExecutor() as pool:
            return list(
                itertools.chain.from_iterable(
                    filter(
                        None,
                        pool.map(
                            func_with_params,
                            iterutils.chunked_iter(iterable, chunk_size),
                        ),
                    )
                ),
            )

    return wrapper


T = TypeVar("T")


def run_batch_operation(
    func: Callable[[str], T], ids: Sequence[str]
) -> Tuple[Sequence[Tuple[str, T]], Sequence[dict]]:
    results = list()
    failures = list()
    for _id in ids:
        try:
            results.append((_id, func(_id)))
        except APIError as err:
            failures.append(
                {
                    "id": _id,
                    "error": {
                        "codes": [err.code, err.subcode],
                        "msg": err.msg,
                        "data": err.error_data,
                    },
                }
            )
    return results, failures
