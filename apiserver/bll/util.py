import functools
import itertools
from concurrent.futures.thread import ThreadPoolExecutor
from typing import (
    Optional,
    Callable,
    Dict,
    Any,
    Set,
    Iterable,
    Tuple,
    Sequence,
    TypeVar,
)

from boltons import iterutils

from apiserver.apierrors import APIError
from apiserver.database.model import AttributedDocument
from apiserver.database.model.settings import Settings


class SetFieldsResolver:
    """
    The class receives set fields dictionary
    and for the set fields that require 'min' or 'max'
    operation replace them with a simple set in case the
    DB document does not have these fields set
    """

    SET_MODIFIERS = ("min", "max")

    def __init__(self, set_fields: Dict[str, Any]):
        self.orig_fields = {}
        self.fields = {}
        self.add_fields(**set_fields)

    def add_fields(self, **set_fields: Any):
        self.orig_fields.update(set_fields)
        self.fields.update(
            {
                f: fname
                for f, modifier, dunder, fname in (
                    (f,) + f.partition("__") for f in set_fields.keys()
                )
                if dunder and modifier in self.SET_MODIFIERS
            }
        )

    def _get_updated_name(self, doc: AttributedDocument, name: str) -> str:
        if name in self.fields and doc.get_field_value(self.fields[name]) is None:
            return self.fields[name]
        return name

    def get_fields(self, doc: AttributedDocument):
        """
        For the given document return the set fields instructions
        with min/max operations replaced with a single set in case
        the document does not have the field set
        """
        return {
            self._get_updated_name(doc, name): value
            for name, value in self.orig_fields.items()
        }

    def get_names(self) -> Set[str]:
        """
        Returns the names of the fields that had min/max modifiers
        in the format suitable for projection (dot separated)
        """
        return set(name.replace("__", ".") for name in self.fields.values())


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
