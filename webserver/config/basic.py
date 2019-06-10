import logging
from pathlib import Path

from pyhocon import ConfigTree, ConfigFactory
from pyparsing import (
    ParseFatalException,
    ParseException,
    RecursiveGrammarException,
    ParseSyntaxException,
)


class BasicConfig:
    NotSet = object()

    def __init__(self, folder):
        self.folder = Path(folder)
        if not self.folder.is_dir():
            raise ValueError("Invalid configuration folder")

        self.prefix = "trains"

        self._load()

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=NotSet):
        value = self._config.get(key, default)
        if value is self.NotSet and not default:
            raise KeyError(
                f"Unable to find value for key '{key}' and default value was not provided."
            )
        return value

    def logger(self, name):
        if Path(name).is_file():
            name = Path(name).stem
        path = ".".join((self.prefix, Path(name).stem))
        return logging.getLogger(path)

    def _load(self, verbose=True):
        self._config = self._read_recursive(self.folder, verbose=verbose)

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
            print("Loading config from {conf_root}")

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
