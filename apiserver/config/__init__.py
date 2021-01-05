import logging.config
from pathlib import Path

from .basic import BasicConfig

config = BasicConfig(Path(__file__).with_name("default"))

logging.config.dictConfig(config.get("logging"))
