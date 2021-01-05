import logging.config
from pathlib import Path

from .basic import BasicConfig


def load_config():
    config = BasicConfig(Path(__file__).with_name("default"))
    logging.config.dictConfig(config.get("logging"))
    return config
