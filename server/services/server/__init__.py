from pyhocon.config_tree import NoneValue

from config import config
from config.info import get_version, get_build_number, get_commit_number
from service_repo import ServiceRepo, APICall, endpoint


@endpoint("server.config")
def get_config(call: APICall):
    path = call.data.get("path")
    if path:
        c = dict(config.get(path))
    else:
        c = config.to_dict()

    def remove_none_value(x):
        """
        Pyhocon bug in Python 3: leaves dummy "NoneValue"s in tree,
        see: https://github.com/chimpler/pyhocon/issues/111
        """
        if isinstance(x, dict):
            return {key: remove_none_value(value) for key, value in x.items()}
        if isinstance(x, list):
            return list(map(remove_none_value, x))
        if isinstance(x, NoneValue):
            return None
        return x

    c.pop("secure", None)

    call.result.data = remove_none_value(c)


@endpoint("server.endpoints")
def get_endpoints(call: APICall):
    call.result.data = ServiceRepo.endpoints_summary()


@endpoint("server.info")
def info(call: APICall):
    call.result.data = {
        "version": get_version(),
        "build": get_build_number(),
        "commit": get_commit_number(),
    }
