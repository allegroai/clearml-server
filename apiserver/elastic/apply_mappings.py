#!/usr/bin/env python3
"""
Apply elasticsearch mappings to given hosts.
"""
import argparse
import json
import logging
from pathlib import Path
from typing import Optional, Sequence, Tuple

from elasticsearch import Elasticsearch, exceptions

HERE = Path(__file__).resolve().parent
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)


def apply_mappings_to_cluster(
    hosts: Sequence,
    key: Optional[str] = None,
    es_args: dict = None,
    http_auth: Tuple = None,
):
    """Hosts maybe a sequence of strings or dicts in the form {"host": <host>, "port": <port>}"""

    def _send_component_template(ct_file):
        with ct_file.open() as json_data:
            body = json.load(json_data)
            template_name = f"{ct_file.stem}"
            res = es.cluster.put_component_template(name=template_name, body=body)
            return {"component_template": template_name, "result": res}

    def _send_index_template(it_file):
        with it_file.open() as json_data:
            body = json.load(json_data)
            template_name = f"{it_file.stem}"
            res = es.indices.put_index_template(name=template_name, body=body)
            return {"index_template": template_name, "result": res}

    # def _send_legacy_template(f):
    #     with f.open() as json_data:
    #         data = json.load(json_data)
    #         template_name = f.stem
    #         res = es.indices.put_template(name=template_name, body=data)
    #         return {"mapping": template_name, "result": res}

    def _delete_legacy_templates(legacy_folder):
        res_list = []
        for lt in legacy_folder.glob("*.json"):
            template_name = lt.stem
            try:
                if not es.indices.get_template(name=template_name):
                    continue
                res = es.indices.delete_template(name=template_name)
            except exceptions.NotFoundError:
                continue
            res_list.append({"deleted legacy mapping": template_name, "result": res})

        return res_list

    es = Elasticsearch(hosts=hosts, http_auth=http_auth, **(es_args or {}))
    root = HERE / "index_templates"
    if key:
        folders = [root / key]
    else:
        folders = [f for f in root.iterdir() if f.is_dir()]

    ret = []
    for f in folders:
        for ct in (f / "component_templates").glob("*.json"):
            ret.append(_send_component_template(ct))
        for it in f.glob("*.json"):
            ret.append(_send_index_template(it))

    legacy_root = HERE / "mappings"
    for f in folders:
        legacy_f = legacy_root / f.stem
        if not legacy_f.exists() or not legacy_f.is_dir():
            continue
        ret.extend(_delete_legacy_templates(legacy_f))

    return ret
    # p = HERE / "mappings"
    # if key:
    #     files = (p / key).glob("*.json")
    # else:
    #     files = p.glob("**/*.json")
    #
    # return [_send_template(f) for f in files]


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--key", help="host key, e.g. events, datasets etc.")
    parser.add_argument(
        "--hosts",
        nargs="+",
        help="list of es hosts from the same cluster, where each host is http[s]://[user:password@]host:port",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(">>>>> Applying mapping to " + str(args.hosts))
    res = apply_mappings_to_cluster(args.hosts, args.key)
    print(res)


if __name__ == "__main__":
    main()
