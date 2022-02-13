import logging
import logging.config
import os
import platform
from functools import reduce
from os import getenv
from os.path import expandvars
from pathlib import Path
from typing import List, Any, TypeVar, Sequence

from boltons.iterutils import first
from pyhocon import ConfigTree, ConfigFactory, ConfigValues
from pyparsing import (
    ParseFatalException,
    ParseException,
    RecursiveGrammarException,
    ParseSyntaxException,
)

from apiserver.utilities import json

EXTRA_CONFIG_PATHS = ("/opt/trains/config", "/opt/clearml/config")
DEFAULT_PREFIXES = ("clearml", "trains")
EXTRA_CONFIG_PATH_SEP = ":" if platform.system() != "Windows" else ";"


class BasicConfig:
    NotSet = object()

    extra_config_values_env_key_sep = "__"
    default_config_dir = "default"

    def __init__(
        self,
        folder: str = None,
        verbose: bool = True,
        prefix: Sequence[str] = DEFAULT_PREFIXES,
    ):
        folder = (
            Path(folder)
            if folder
            else Path(__file__).with_name(self.default_config_dir)
        )
        if not folder.is_dir():
            raise ValueError("Invalid configuration folder")

        self.verbose = verbose

        self.extra_config_path_override_var = [
            f"{p.upper()}_CONFIG_DIR" for p in prefix
        ]

        self.prefix = prefix[0]
        self.extra_config_values_env_key_prefix = [
            f"{p.upper()}{self.extra_config_values_env_key_sep}"
            for p in reversed(prefix)
        ]

        self._paths = [folder, *self._get_paths()]
        self._config = self._reload()

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key: str, default: Any = NotSet) -> Any:
        value = self._config.get(key, default)
        if value is self.NotSet:
            raise KeyError(
                f"Unable to find value for key '{key}' and default value was not provided."
            )
        return value

    def to_dict(self) -> dict:
        return self._config.as_plain_ordered_dict()

    def as_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def logger(self, name: str) -> logging.Logger:
        if Path(name).is_file():
            name = Path(name).stem
            if name == "__init__" and Path(name).parent.stem:
                name = Path(name).parent.stem
        path = ".".join((self.prefix, name))
        return logging.getLogger(path)

    def _read_extra_env_config_values(self) -> ConfigTree:
        """ Loads extra configuration from environment-injected values """
        result = ConfigTree()

        for prefix in self.extra_config_values_env_key_prefix:
            keys = sorted(k for k in os.environ if k.startswith(prefix))
            for key in keys:
                path = (
                    key[len(prefix) :]
                    .replace(self.extra_config_values_env_key_sep, ".")
                    .lower()
                )
                result = self._merge_configs(
                    result, ConfigFactory.parse_string(f"{path}: {os.environ[key]}")
                )

        return result

    def _get_paths(self) -> List[Path]:
        default_paths = EXTRA_CONFIG_PATH_SEP.join(EXTRA_CONFIG_PATHS)
        value = first(map(getenv, self.extra_config_path_override_var), default_paths)

        paths = [
            Path(expandvars(v)).expanduser() for v in value.split(EXTRA_CONFIG_PATH_SEP)
        ]

        if value is not default_paths:
            invalid = [path for path in paths if not path.is_dir()]
            if invalid:
                print(
                    f"WARNING: Invalid paths in {self.extra_config_path_override_var} env var: {' '.join(map(str, invalid))}"
                )

        return [path for path in paths if path.is_dir()]

    def reload(self):
        self._config = self._reload()

    def _reload(self) -> ConfigTree:
        extra_config_values = self._read_extra_env_config_values()

        configs = [self._read_recursive(path) for path in self._paths]

        return reduce(
            lambda last, config: self._merge_configs(
                last, config, copy_trees=True
            ),
            configs + [extra_config_values],
            ConfigTree(),
        )

    @classmethod
    def _merge_configs(cls, a, b, copy_trees=False, override_prefix="-"):
        """Based on pyhocon.ConfigTree.merge_configs, with dict override support using a `-` key prefix"""
        for key, value in b.items():
            override = key.startswith(override_prefix)
            if override:
                key = key[len(override_prefix):]
            # if key is in both a and b and both values are dictionary then merge it otherwise override it
            if not override and key in a and isinstance(a[key], ConfigTree) and isinstance(b[key], ConfigTree):
                if copy_trees:
                    a[key] = a[key].copy()
                cls._merge_configs(a[key], b[key], copy_trees=copy_trees)
            else:
                if isinstance(value, ConfigValues):
                    value.parent = a
                    value.key = key
                    if key in a:
                        value.overriden_value = a[key]
                a[key] = value
                if a.root:
                    if b.root:
                        a.history[key] = a.history.get(key, []) + b.history.get(key, [value])
                    else:
                        a.history[key] = a.history.get(key, []) + [value]

        return a

    def _read_recursive(self, conf_root) -> ConfigTree:
        conf = ConfigTree()

        if not conf_root:
            return conf

        if not conf_root.is_dir():
            if self.verbose:
                if not conf_root.exists():
                    print(f"No config in {conf_root}")
                else:
                    print(f"Not a directory: {conf_root}")
            return conf

        if self.verbose:
            print(f"Loading config from {conf_root}")

        for file in conf_root.rglob("*.conf"):
            key = ".".join(file.relative_to(conf_root).with_suffix("").parts)
            conf.put(key, self._read_single_file(file))

        return conf

    def _read_single_file(self, file_path):
        if self.verbose:
            print(f"Loading config from file {file_path}")

        try:
            return ConfigFactory.parse_file(file_path)
        except ParseSyntaxException as ex:
            msg = f"Failed parsing {file_path} ({ex.__class__.__name__}): (at char {ex.loc}, line:{ex.lineno}, col:{ex.column})"
            raise ConfigurationError(msg, file_path=file_path) from ex
        except (ParseException, ParseFatalException, RecursiveGrammarException) as ex:
            msg = f"Failed parsing {file_path} ({ex.__class__.__name__}): {ex}"
            raise ConfigurationError(msg) from ex
        except Exception as ex:
            print(f"Failed loading {file_path}: {ex}")
            raise

    def initialize_logging(self):
        logging_config = self.get("logging", None)
        if not logging_config:
            return
        logging.config.dictConfig(logging_config)


class ConfigurationError(Exception):
    def __init__(self, msg, file_path=None, *args):
        super().__init__(msg, *args)
        self.file_path = file_path


ConfigType = TypeVar("ConfigType", bound=BasicConfig)
