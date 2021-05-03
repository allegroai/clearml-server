from typing import Sequence, Tuple, Any, Union, Callable, Optional, Mapping


def flatten_nested_items(
    dictionary: dict, nesting: int = None, include_leaves=None, prefix=None
) -> Sequence[Tuple[Tuple[str, ...], Any]]:
    """
    iterate through dictionary and return with nested keys flattened into a tuple
    """
    next_nesting = None if nesting is None else (nesting - 1)
    prefix = prefix or ()
    for key, value in dictionary.items():
        path = prefix + (key,)
        if isinstance(value, dict) and nesting != 0:
            yield from flatten_nested_items(
                value, next_nesting, include_leaves, prefix=path
            )
        elif include_leaves is None or key in include_leaves:
            yield path, value


def deep_merge(source: dict, override: dict) -> dict:
    """
    Merge the override dict into the source in-place
    Contrary to the dpath.merge the sequences are not expanded
    If override contains the sequence with the same name as source
    then the whole sequence in the source is overridden
    """
    for key, value in override.items():
        if key in source and isinstance(source[key], dict) and isinstance(value, dict):
            deep_merge(source[key], value)
        else:
            source[key] = value

    return source


def nested_get(
    dictionary: Mapping,
    path: Union[Sequence[str], str],
    default: Optional[Union[Any, Callable]] = None,
) -> Any:
    if isinstance(path, str):
        path = [path]

    node = dictionary
    for key in path:
        if key not in node:
            if callable(default):
                return default()
            return default
        node = node.get(key)

    return node


def nested_delete(dictionary: dict, path: Union[Sequence[str], str]) -> bool:
    """
    Return 'True' if the element was deleted
    """
    if isinstance(path, str):
        path = [path]

    *parent_path, last_key = path
    parent = nested_get(dictionary, parent_path)
    if not parent or last_key not in parent:
        return False

    del parent[last_key]
    return True


def nested_set(dictionary: dict, path: Union[Sequence[str], str], value: Any):
    if isinstance(path, str):
        path = [path]

    *parent_path, last_key = path
    node = dictionary
    for key in parent_path:
        if key not in node:
            node[key] = {}
        node = node.get(key)

    node[last_key] = value
