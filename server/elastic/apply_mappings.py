#!/usr/bin/env python3
"""
Apply elasticsearch mappings to given hosts.
"""
import argparse
import json
import requests
from pathlib import Path

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

HERE = Path(__file__).parent


def apply_mappings_to_host(host: str):
    def _send_mapping(f):
        with f.open() as json_data:
            data = json.load(json_data)
            es_server = host
            url = f"{es_server}/_template/{f.stem}"

            session = requests.Session()
            adapter = HTTPAdapter(max_retries=Retry(5, backoff_factor=0.5))
            session.mount('http://', adapter)

            session.delete(url)
            r = session.post(
                url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(data),
            )
            return {"mapping": f.stem, "result": r.text}

    p = HERE / "mappings"
    return [
        _send_mapping(f) for f in p.iterdir() if f.is_file() and f.suffix == ".json"
    ]


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("hosts", nargs="+")
    return parser.parse_args()


def main():
    for host in parse_args().hosts:
        print(">>>>> Applying mapping to " + host)
        res = apply_mappings_to_host(host)
        print(res)


if __name__ == "__main__":
    main()
