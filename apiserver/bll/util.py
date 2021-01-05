import functools
import itertools
from concurrent.futures.thread import ThreadPoolExecutor
from operator import itemgetter
from typing import Sequence, Optional, Callable, Tuple, Dict, Any, Set, Iterable

from boltons import iterutils

from apiserver.database.model import AttributedDocument
from apiserver.database.model.settings import Settings


def extract_properties_to_lists(
    key_names: Sequence[str],
    data: Sequence[dict],
    extract_func: Optional[Callable[[dict], Tuple]] = None,
) -> dict:
    """
    Given a list of dictionaries and names of dictionary keys
    builds a dictionary with the requested keys and values lists
    :param key_names: names of the keys in the resulting dictionary
    :param data: sequence of dictionaries to extract values from
    :param extract_func: the optional callable that extracts properties
    from a dictionary and put them in a tuple in the order corresponding to
    key_names. If not specified then properties are extracted according to key_names
    """
    value_sequences = zip(*map(extract_func or itemgetter(*key_names), data))
    return dict(zip(key_names, map(list, value_sequences)))


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
