from functools import reduce
from pathlib import Path
from typing import Union

from pyhocon import ConfigFactory, ConfigTree

from .generator import Generator


class ErrorsGenerator:
    _apierrors_path = Path(__file__).parents[1] / "apierrors"
    _files = [_apierrors_path / "errors.conf"]

    @classmethod
    def _get_codes(cls):
        return {
            (k, v.pop("_")): v
            for k, v in reduce(
                ConfigTree.merge_configs, map(ConfigFactory.parse_file, cls._files),
            ).items()
        }

    @classmethod
    def add_errors_file(cls, path: Union[Path, str]):
        cls._files.append(path)

    @classmethod
    def generate_python_files(cls):
        Generator(cls._apierrors_path / "errors", format_pep8=False).make_errors(
            cls._get_codes()
        )
