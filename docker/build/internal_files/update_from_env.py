#!/usr/bin/env python3
""" Update json configuration file from environment variables """
from argparse import ArgumentParser, FileType
import json
from os import environ
from typing import Any, Generator, Tuple, Optional, List


class PathConflictError(Exception):
    def __init__(self, path_: List[str]):
        self.path = path_


def scan(
    obj: Any, path_: str = None, sep: str = ".", parent_=None, key_=None,
) -> Generator[Tuple[str, Any, Optional[dict], str], None, None]:
    if not isinstance(obj, dict):
        yield path_.lower(), obj, parent_, key_
    else:
        for k, v in obj.items():
            yield from scan(v, path_=sep.join(filter(None, (path_, k))), parent_=obj, key_=k, sep=sep)


def set_path(p: List[str], obj: dict, v: Any):
    key_, *rest = p
    if not rest:
        obj[key_] = v
    else:
        if key_ in obj:
            if not isinstance(obj[key_], dict):
                raise PathConflictError(rest)
        else:
            obj[key_] = {}
        return set_path(rest, obj[key_], v)


if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("input_file", type=FileType(), help="Input JSON file")
    parser.add_argument("output_file", type=FileType("w"), help="Output JSON file")
    parser.add_argument(
        "--env-prefix", "-p", default="WEBSERVER", help="Environment variables prefix (default=%(default)s)",
        dest="prefix", required=False
    )
    parser.add_argument(
        "--env-separator", "-s", default="__", help="Environment variable name separator (default=%(default)s)",
        dest="sep"
    )
    parser.add_argument("--verbose", "-v", action="store_true", default=False)
    parser.add_argument(
        "--disable-parse-env-value", action="store_false", default=True, help="Don't parse env value as JSON",
        dest="parse_env"
    )

    args = parser.parse_args()

    if not args.prefix:
        print("Error: script does not support an empty prefix")
        exit(1)

    data = None
    try:
        data = json.load(args.input_file)
    except json.JSONDecodeError as ex:
        print(f"Error parsing JSON file {args.input_file.name}: {str(ex)}")
        exit(1)

    def parse_value(k, v):
        try:
            return json.loads(v)
        except json.JSONDecodeError as ex:
            print(f"Error parsing {k} JSON value `{v}`: {str(ex)}")
            exit(2)

    prefix = args.prefix + args.sep

    env_vars = {
        k.lstrip(prefix): parse_value(k, v) if args.parse_env else v
        for k, v in environ.items() if k.startswith(prefix)
    }

    for path, value, parent, key in scan(data, sep=args.sep):
        if not (parent and key):
            continue

        match = next((k for k in env_vars if k.lower() == path), None)
        if match:
            replace = env_vars.pop(match)
            parent[key] = replace
            if args.verbose:
                print(f"Replacing {path}={value} with {replace}")

    for k, v in env_vars.items():
        path = k.split(args.sep)
        try:
            set_path(path, data, v)
        except PathConflictError as ex:
            print(f"Error: failed setting value into {k}: {path[:-len(ex.path)]} is not a dictionary")

    try:
        json.dump(data, args.output_file, sort_keys=True, indent=2)
    except Exception as ex:
        print(f"Error writing JSON file {args.output_file.name}: {str(ex)}")
        exit(3)
