from operator import itemgetter
from typing import Sequence, Optional, Callable, Tuple


def strict_map(*args, **kwargs):
    return list(map(*args, **kwargs))


def extract_properties_to_lists(
    key_names: Sequence[str],
    data: Sequence[dict],
    extract_func: Optional[Callable[[dict], Tuple]] = None,
    target_keys: Optional[Sequence[str]] = None,
) -> dict:
    """
    Given a list of dictionaries and names of dictionary keys
    builds a dictionary with the requested keys and values lists
    For the empty list return the dictionary of empty lists
    :param key_names: names of the keys in the resulting dictionary
    :param data: sequence of dictionaries to extract values from
    :param extract_func: the optional callable that extracts properties
    from a dictionary and put them in a tuple in the order corresponding to
    key_names. If not specified then properties are extracted according to key_names
    :param target_keys: optional alternative keys to use in the target dictionary. must be equal in length to key_names.
    """
    if not data:
        return {k: [] for k in key_names}

    value_sequences = zip(*map(extract_func or itemgetter(*key_names), data))
    return dict(zip((target_keys or key_names), map(list, value_sequences)))
