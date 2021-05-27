import logging
import os
from functools import reduce
from os import getenv
from os.path import expandvars
from pathlib import Path

from boltons.iterutils import first
from pyhocon import ConfigTree, ConfigFactory
from pyparsing import (
    ParseFatalException,
    ParseException,
    RecursiveGrammarException,
    ParseSyntaxException,
)

DEFAULT_EXTRA_CONFIG_PATH = "/opt/clearml/config"
PREFIXES = ("CLEARML", "TRAINS")
EXTRA_CONFIG_PATH_SEP = ":"
EXTRA_CONFIG_VALUES_ENV_KEY_SEP = "__"


class BasicConfig:
    NotSet = object()

    def __init__(self, folder):
        self.folder = Path(folder)
        if not self.folder.is_dir():
            raise ValueError("Invalid configuration folder")

        self.extra_config_path_env_key = [
            f"{p.upper()}_CONFIG_DIR" for p in PREFIXES
        ]

        self.prefix = PREFIXES[0]
        self.extra_config_values_env_key_prefix = [
            f"{p.upper()}{EXTRA_CONFIG_VALUES_ENV_KEY_SEP}"
            for p in reversed(PREFIXES)
        ]

        self._load()

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=NotSet):
        value = self._config.get(key, default)
        if value is self.NotSet:
            raise KeyError(
                f"Unable to find value for key '{key}' and default value was not provided."
            )
        return value

    def logger(self, name):
        if Path(name).is_file():
            name = Path(name).stem
        path = ".".join((self.prefix, Path(name).stem))
        return logging.getLogger(path)

    def _read_extra_env_config_values(self):
        """ Loads extra configuration from environment-injected values """
        result = ConfigTree()

        for prefix in self.extra_config_values_env_key_prefix:
            keys = sorted(k for k in os.environ if k.startswith(prefix))
            for key in keys:
                path = key[len(prefix) :].replace(EXTRA_CONFIG_VALUES_ENV_KEY_SEP, ".").lower()
                result = ConfigTree.merge_configs(
                    result, ConfigFactory.parse_string(f"{path}: {os.environ[key]}")
                )

        return result

    def _read_env_paths(self):
        value = first(map(getenv, self.extra_config_path_env_key), DEFAULT_EXTRA_CONFIG_PATH)
        if value is None:
            return
        paths = [
            Path(expandvars(v)).expanduser() for v in value.split(EXTRA_CONFIG_PATH_SEP)
        ]
        invalid = [
            path
            for path in paths
            if not path.is_dir() and str(path) != DEFAULT_EXTRA_CONFIG_PATH
        ]
        if invalid:
            print(f"WARNING: Invalid paths in {self.extra_config_path_env_key} env var: {' '.join(map(str,invalid))}")
        return [path for path in paths if path.is_dir()]

    def _load(self, verbose=True):
        extra_config_paths = self._read_env_paths() or []
        extra_config_values = self._read_extra_env_config_values()
        configs = [
            self._read_recursive(path, verbose=verbose)
            for path in [self.folder] + extra_config_paths
        ]

        self._config = reduce(
            lambda last, config: ConfigTree.merge_configs(
                last, config, copy_trees=True
            ),
            configs + [extra_config_values],
            ConfigTree(),
        )

    def _read_recursive(self, conf_root, verbose=True):
        conf = ConfigTree()

        if not conf_root:
            return conf

        if not conf_root.is_dir():
            if verbose:
                if not conf_root.exists():
                    print(f"No config in {conf_root}")
                else:
                    print(f"Not a directory: {conf_root}")
            return conf

        if verbose:
            print(f"Loading config from {conf_root}")

        for file in conf_root.rglob("*.conf"):
            key = ".".join(file.relative_to(conf_root).with_suffix("").parts)
            conf.put(key, self._read_single_file(file, verbose=verbose))

        return conf

    @staticmethod
    def _read_single_file(file_path, verbose=True):
        if verbose:
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


class ConfigurationError(Exception):
    def __init__(self, msg, file_path=None, *args):
        super(ConfigurationError, self).__init__(msg, *args)
        self.file_path = file_path
