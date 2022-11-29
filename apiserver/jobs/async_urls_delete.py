from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from pathlib import Path
from time import sleep
from typing import Sequence, Tuple

import requests
from furl import furl
from mongoengine import Q

from apiserver.config_repo import config
from apiserver.database import db
from apiserver.database.model.url_to_delete import (
    UrlToDelete,
    DeletionStatus,
    StorageType,
)

log = config.logger(f"JOB-{Path(__file__).name}")
conf = config.get("services.async_urls_delete")
max_retries = conf.get("max_retries", 3)
retry_timeout = timedelta(seconds=conf.get("retry_timeout_sec", 60))
fileserver_timeout = conf.get("fileserver.timeout_sec", 300)
UrlPrefix = Tuple[str, str]


def validate_fileserver_access(fileserver_host: str) -> str:
    fileserver_host = fileserver_host or config.get("hosts.fileserver", None)
    if not fileserver_host:
        log.error(f"Fileserver host not configured")
        exit(1)

    res = requests.get(url=fileserver_host)
    res.raise_for_status()

    return fileserver_host


def mark_retry_failed(ids: Sequence[str], reason: str):
    UrlToDelete.objects(id__in=ids).update(
        last_failure_time=datetime.utcnow(),
        last_failure_reason=reason,
        inc__retry_count=1,
    )
    UrlToDelete.objects(id__in=ids, retry_count__gte=max_retries).update(
        status=DeletionStatus.failed
    )


def mark_failed(query: Q, reason: str):
    UrlToDelete.objects(query).update(
        status=DeletionStatus.failed,
        last_failure_time=datetime.utcnow(),
        last_failure_reason=reason,
    )


def delete_fileserver_urls(
    urls_query: Q, fileserver_host: str, url_prefixes: Sequence[UrlPrefix]
):
    to_delete = list(UrlToDelete.objects(urls_query).limit(10000))
    if not to_delete:
        return

    def resolve_path(url_: UrlToDelete) -> str:
        parsed = furl(url_.url)
        url_host = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else None
        url_path = str(parsed.path)

        for host, path_prefix in url_prefixes:
            if host and url_host != host:
                continue
            if path_prefix and not url_path.startswith(path_prefix + "/"):
                continue
            return url_path[len(path_prefix or ""):]

        raise ValueError("could not map path")

    paths = set()
    path_to_id_mapping = defaultdict(list)
    for url in to_delete:
        try:
            path = resolve_path(url)
            path = path.strip("/")
            if not path:
                raise ValueError("Empty path")
        except Exception as ex:
            err = str(ex)
            log.warn(f"Error getting path for {url.url}: {err}")
            mark_failed(Q(id=url.id), err)
            continue

        paths.add(path)
        path_to_id_mapping[path].append(url.id)

    if not paths:
        return

    ids_to_delete = set(chain.from_iterable(path_to_id_mapping.values()))
    try:
        res = requests.post(
            url=furl(fileserver_host).add(path="delete_many").url,
            json={"files": list(paths)},
            timeout=fileserver_timeout,
        )
        res.raise_for_status()
    except Exception as ex:
        err = str(ex)
        log.warn(f"Error deleting {len(paths)} files from fileserver: {err}")
        mark_retry_failed(list(ids_to_delete), err)
        return

    res_data = res.json()
    deleted_ids = set(
        chain.from_iterable(
            path_to_id_mapping.get(path, [])
            for path in list(res_data.get("deleted", {}))
        )
    )
    if deleted_ids:
        UrlToDelete.objects(id__in=list(deleted_ids)).delete()
        log.info(f"{len(deleted_ids)} files deleted from the fileserver")

    failed_ids = set()
    for err, error_ids in res_data.get("errors", {}).items():
        error_ids = list(
            chain.from_iterable(path_to_id_mapping.get(path, []) for path in error_ids)
        )
        mark_retry_failed(error_ids, err)
        log.warning(
            f"Failed to delete {len(error_ids)} files from the fileserver due to: {err}"
        )
        failed_ids.update(error_ids)

    missing_ids = ids_to_delete - deleted_ids - failed_ids
    if missing_ids:
        mark_retry_failed(list(missing_ids), "Not succeeded")


def _get_fileserver_url_prefixes(fileserver_host: str) -> Sequence[UrlPrefix]:
    def _parse_url_prefix(prefix) -> UrlPrefix:
        url = furl(prefix)
        host = f"{url.scheme}://{url.netloc}" if url.scheme else None
        return host, str(url.path).rstrip("/")

    url_prefixes = [
        _parse_url_prefix(p) for p in conf.get("fileserver.url_prefixes", [])
    ]
    if not any(fileserver_host == host for host, _ in url_prefixes):
        url_prefixes.append((fileserver_host, ""))

    return url_prefixes


def run_delete_loop(fileserver_host: str):
    fileserver_host = validate_fileserver_access(fileserver_host)

    storage_delete_funcs = {
        StorageType.fileserver: partial(
            delete_fileserver_urls,
            fileserver_host=fileserver_host,
            url_prefixes=_get_fileserver_url_prefixes(fileserver_host),
        ),
    }
    while True:
        now = datetime.utcnow()
        urls_query = (
            Q(status__ne=DeletionStatus.failed)
            & Q(retry_count__lt=max_retries)
            & (
                Q(last_failure_time__exists=False)
                | Q(last_failure_time__lt=now - retry_timeout)
            )
        )

        url_to_delete: UrlToDelete = UrlToDelete.objects(
            urls_query & Q(storage_type__in=list(storage_delete_funcs))
        ).order_by("retry_count").limit(1).first()
        if not url_to_delete:
            sleep(10)
            continue

        company = url_to_delete.company
        user = url_to_delete.user
        storage_type = url_to_delete.storage_type
        log.info(
            f"Deleting {storage_type} objects for company: {company}, user: {user}"
        )
        company_storage_urls_query = urls_query & Q(
            company=company, storage_type=storage_type,
        )
        storage_delete_funcs[storage_type](urls_query=company_storage_urls_query)


def main():
    parser = ArgumentParser(description=__doc__)

    parser.add_argument(
        "--fileserver-host", "-fh", help="Fileserver host address", type=str,
    )
    args = parser.parse_args()

    db.initialize()
    run_delete_loop(args.fileserver_host)


if __name__ == "__main__":
    main()
