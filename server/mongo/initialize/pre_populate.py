import hashlib
import importlib
import os
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from itertools import chain
from operator import attrgetter
from os.path import splitext
from pathlib import Path
from typing import Optional, Any, Type, Set, Dict, Sequence, Tuple, BinaryIO, Union
from urllib.parse import unquote, urlparse
from zipfile import ZipFile, ZIP_BZIP2

import attr
import mongoengine
from boltons.iterutils import chunked_iter
from furl import furl
from mongoengine import Q

from bll.event import EventBLL
from database.model import EntityVisibility
from database.model.model import Model
from database.model.project import Project
from database.model.task.task import Task, ArtifactModes, TaskStatus
from database.utils import get_options
from utilities import json


class PrePopulate:
    event_bll = EventBLL()
    events_file_suffix = "_events"
    export_tag_prefix = "Exported:"
    export_tag = f"{export_tag_prefix} %Y-%m-%d %H:%M:%S"

    class JsonLinesWriter:
        def __init__(self, file: BinaryIO):
            self.file = file
            self.empty = True

        def __enter__(self):
            self._write("[")
            return self

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self._write("\n]")

        def _write(self, data: str):
            self.file.write(data.encode("utf-8"))

        def write(self, line: str):
            if not self.empty:
                self._write(",")
            self._write("\n" + line)
            self.empty = False

    @attr.s(auto_attribs=True)
    class _MapData:
        files: Sequence[str] = None
        entities: Dict[str, datetime] = None

    @staticmethod
    def _get_last_update_time(entity) -> datetime:
        return getattr(entity, "last_update", None) or getattr(entity, "created")

    @classmethod
    def _check_for_update(
        cls, map_file: Path, entities: dict
    ) -> Tuple[bool, Sequence[str]]:
        if not map_file.is_file():
            return True, []

        files = []
        try:
            map_data = cls._MapData(**json.loads(map_file.read_text()))
            files = map_data.files
            for file in files:
                if not Path(file).is_file():
                    return True, files

            new_times = {
                item.id: cls._get_last_update_time(item).replace(tzinfo=timezone.utc)
                for item in chain.from_iterable(entities.values())
            }
            old_times = map_data.entities

            if set(new_times.keys()) != set(old_times.keys()):
                return True, files

            for id_, new_timestamp in new_times.items():
                if new_timestamp != old_times[id_]:
                    return True, files
        except Exception as ex:
            print("Error reading map file. " + str(ex))
            return True, files

        return False, files

    @classmethod
    def _write_update_file(
        cls, map_file: Path, entities: dict, created_files: Sequence[str]
    ):
        map_data = cls._MapData(
            files=created_files,
            entities={
                entity.id: cls._get_last_update_time(entity)
                for entity in chain.from_iterable(entities.values())
            },
        )
        map_file.write_text(json.dumps(attr.asdict(map_data)))

    @staticmethod
    def _filter_artifacts(artifacts: Sequence[str]) -> Sequence[str]:
        def is_fileserver_link(a: str) -> bool:
            a = a.lower()
            if a.startswith("https://files."):
                return True
            if a.startswith("http"):
                parsed = urlparse(a)
                if parsed.scheme in {"http", "https"} and parsed.port == 8081:
                    return True
            return False

        fileserver_links = [a for a in artifacts if is_fileserver_link(a)]
        print(
            f"Found {len(fileserver_links)} files on the fileserver from {len(artifacts)} total"
        )

        return fileserver_links

    @classmethod
    def export_to_zip(
        cls,
        filename: str,
        experiments: Sequence[str] = None,
        projects: Sequence[str] = None,
        artifacts_path: str = None,
        task_statuses: Sequence[str] = None,
        tag_exported_entities: bool = False,
    ) -> Sequence[str]:
        if task_statuses and not set(task_statuses).issubset(get_options(TaskStatus)):
            raise ValueError("Invalid task statuses")

        file = Path(filename)
        entities = cls._resolve_entities(
            experiments=experiments, projects=projects, task_statuses=task_statuses
        )

        map_file = file.with_suffix(".map")
        updated, old_files = cls._check_for_update(map_file, entities)
        if not updated:
            print(f"There are no updates from the last export")
            return old_files
        for old in old_files:
            old_path = Path(old)
            if old_path.is_file():
                old_path.unlink()

        zip_args = dict(mode="w", compression=ZIP_BZIP2)
        with ZipFile(file, **zip_args) as zfile:
            artifacts, hash_ = cls._export(
                zfile, entities, tag_entities=tag_exported_entities
            )
        file_with_hash = file.with_name(f"{file.stem}_{hash_}{file.suffix}")
        file.replace(file_with_hash)
        created_files = [str(file_with_hash)]

        artifacts = cls._filter_artifacts(artifacts)
        if artifacts and artifacts_path and os.path.isdir(artifacts_path):
            artifacts_file = file_with_hash.with_suffix(".artifacts")
            with ZipFile(artifacts_file, **zip_args) as zfile:
                cls._export_artifacts(zfile, artifacts, artifacts_path)
            created_files.append(str(artifacts_file))

        cls._write_update_file(map_file, entities, created_files)

        return created_files

    @classmethod
    def import_from_zip(
        cls, filename: str, company_id: str, user_id: str, artifacts_path: str
    ):
        with ZipFile(filename) as zfile:
            cls._import(zfile, company_id, user_id)

        if artifacts_path and os.path.isdir(artifacts_path):
            artifacts_file = Path(filename).with_suffix(".artifacts")
            if artifacts_file.is_file():
                print(f"Unzipping artifacts into {artifacts_path}")
                with ZipFile(artifacts_file) as zfile:
                    zfile.extractall(artifacts_path)

    @staticmethod
    def _resolve_type(
        cls: Type[mongoengine.Document], ids: Optional[Sequence[str]]
    ) -> Sequence[Any]:
        ids = set(ids)
        items = list(cls.objects(id__in=list(ids)))
        resolved = {i.id for i in items}
        missing = ids - resolved
        for name_candidate in missing:
            results = list(cls.objects(name=name_candidate))
            if not results:
                print(f"ERROR: no match for `{name_candidate}`")
                exit(1)
            elif len(results) > 1:
                print(f"ERROR: more than one match for `{name_candidate}`")
                exit(1)
            items.append(results[0])
        return items

    @classmethod
    def _resolve_entities(
        cls,
        experiments: Sequence[str] = None,
        projects: Sequence[str] = None,
        task_statuses: Sequence[str] = None,
    ) -> Dict[Type[mongoengine.Document], Set[mongoengine.Document]]:
        entities = defaultdict(set)

        if projects:
            print("Reading projects...")
            entities[Project].update(cls._resolve_type(Project, projects))
            print("--> Reading project experiments...")
            query = Q(
                project__in=list(set(filter(None, (p.id for p in entities[Project])))),
                system_tags__nin=[EntityVisibility.archived.value],
            )
            if task_statuses:
                query &= Q(status__in=list(set(task_statuses)))
            objs = Task.objects(query)
            entities[Task].update(o for o in objs if o.id not in (experiments or []))

        if experiments:
            print("Reading experiments...")
            entities[Task].update(cls._resolve_type(Task, experiments))
            print("--> Reading experiments projects...")
            objs = Project.objects(
                id__in=list(set(filter(None, (p.project for p in entities[Task]))))
            )
            project_ids = {p.id for p in entities[Project]}
            entities[Project].update(o for o in objs if o.id not in project_ids)

        model_ids = {
            model_id
            for task in entities[Task]
            for model_id in (task.output.model, task.execution.model)
            if model_id
        }
        if model_ids:
            print("Reading models...")
            entities[Model] = set(Model.objects(id__in=list(model_ids)))

        return entities

    @classmethod
    def _filter_out_export_tags(cls, tags: Sequence[str]) -> Sequence[str]:
        if not tags:
            return tags
        return [tag for tag in tags if not tag.startswith(cls.export_tag_prefix)]

    @classmethod
    def _cleanup_model(cls, model: Model):
        model.company = ""
        model.user = ""
        model.tags = cls._filter_out_export_tags(model.tags)

    @classmethod
    def _cleanup_task(cls, task: Task):
        task.comment = "Auto generated by Allegro.ai"
        task.status_message = ""
        task.status_reason = ""
        task.user = ""
        task.company = ""
        task.tags = cls._filter_out_export_tags(task.tags)
        if task.output:
            task.output.destination = None

    @classmethod
    def _cleanup_project(cls, project: Project):
        project.user = ""
        project.company = ""
        project.tags = cls._filter_out_export_tags(project.tags)

    @classmethod
    def _cleanup_entity(cls, entity_cls, entity):
        if entity_cls == Task:
            cls._cleanup_task(entity)
        elif entity_cls == Model:
            cls._cleanup_model(entity)
        elif entity == Project:
            cls._cleanup_project(entity)

    @classmethod
    def _add_tag(cls, items: Sequence[Union[Project, Task, Model]], tag: str):
        try:
            for item in items:
                item.update(upsert=False, tags=sorted(item.tags + [tag]))
        except AttributeError:
            pass

    @classmethod
    def _export_task_events(
        cls, task: Task, base_filename: str, writer: ZipFile, hash_
    ) -> Sequence[str]:
        artifacts = []
        filename = f"{base_filename}_{task.id}{cls.events_file_suffix}.json"
        print(f"Writing task events into {writer.filename}:{filename}")
        with BytesIO() as f:
            with cls.JsonLinesWriter(f) as w:
                scroll_id = None
                while True:
                    res = cls.event_bll.get_task_events(
                        task.company, task.id, scroll_id=scroll_id
                    )
                    if not res.events:
                        break
                    scroll_id = res.next_scroll_id
                    for event in res.events:
                        if event.get("type") == "training_debug_image":
                            url = cls._get_fixed_url(event.get("url"))
                            if url:
                                event["url"] = url
                                artifacts.append(url)
                        w.write(json.dumps(event))
            data = f.getvalue()
            hash_.update(data)
            writer.writestr(filename, data)

        return artifacts

    @staticmethod
    def _get_fixed_url(url: Optional[str]) -> Optional[str]:
        if not (url and url.lower().startswith("s3://")):
            return url
        try:
            fixed = furl(url)
            fixed.scheme = "https"
            fixed.host += ".s3.amazonaws.com"
            return fixed.url
        except Exception as ex:
            print(f"Failed processing link {url}. " + str(ex))
            return url

    @classmethod
    def _export_entity_related_data(
        cls, entity_cls, entity, base_filename: str, writer: ZipFile, hash_
    ):
        if entity_cls == Task:
            return [
                *cls._get_task_output_artifacts(entity),
                *cls._export_task_events(entity, base_filename, writer, hash_),
            ]

        if entity_cls == Model:
            entity.uri = cls._get_fixed_url(entity.uri)
            return [entity.uri] if entity.uri else []

        return []

    @classmethod
    def _get_task_output_artifacts(cls, task: Task) -> Sequence[str]:
        if not task.execution.artifacts:
            return []

        for a in task.execution.artifacts:
            if a.mode == ArtifactModes.output:
                a.uri = cls._get_fixed_url(a.uri)

        return [
            a.uri
            for a in task.execution.artifacts
            if a.mode == ArtifactModes.output and a.uri
        ]

    @classmethod
    def _export_artifacts(
        cls, writer: ZipFile, artifacts: Sequence[str], artifacts_path: str
    ):
        unique_paths = set(unquote(str(furl(artifact).path)) for artifact in artifacts)
        print(f"Writing {len(unique_paths)} artifacts into {writer.filename}")
        for path in unique_paths:
            path = path.lstrip("/")
            full_path = os.path.join(artifacts_path, path)
            if os.path.isfile(full_path):
                writer.write(full_path, path)
            else:
                print(f"Artifact {full_path} not found")

    @classmethod
    def _export(
        cls, writer: ZipFile, entities: dict, tag_entities: bool = False
    ) -> Tuple[Sequence[str], str]:
        """
        Export the requested experiments, projects and models and return the list of artifact files
        Always do the export on sorted items since the order of items influence hash
        """
        artifacts = []
        now = datetime.utcnow()
        hash_ = hashlib.md5()
        for cls_ in sorted(entities, key=attrgetter("__name__")):
            items = sorted(entities[cls_], key=attrgetter("id"))
            if not items:
                continue
            base_filename = f"{cls_.__module__}.{cls_.__name__}"
            for item in items:
                artifacts.extend(
                    cls._export_entity_related_data(
                        cls_, item, base_filename, writer, hash_
                    )
                )
            filename = base_filename + ".json"
            print(f"Writing {len(items)} items into {writer.filename}:{filename}")
            with BytesIO() as f:
                with cls.JsonLinesWriter(f) as w:
                    for item in items:
                        cls._cleanup_entity(cls_, item)
                        w.write(item.to_json())
                data = f.getvalue()
                hash_.update(data)
                writer.writestr(filename, data)

            if tag_entities:
                cls._add_tag(items, now.strftime(cls.export_tag))

        return artifacts, hash_.hexdigest()

    @staticmethod
    def json_lines(file: BinaryIO):
        for line in file:
            clean = (
                line.decode("utf-8")
                .rstrip("\r\n")
                .strip()
                .lstrip("[")
                .rstrip(",]")
                .strip()
            )
            if not clean:
                continue
            yield clean

    @classmethod
    def _import(cls, reader: ZipFile, company_id: str = "", user_id: str = None):
        """
        Import entities and events from the zip file
        Start from entities since event import will require the tasks already in DB
        """
        event_file_ending = cls.events_file_suffix + ".json"
        entity_files = (
            fi
            for fi in reader.filelist
            if not fi.orig_filename.endswith(event_file_ending)
        )
        event_files = (
            fi for fi in reader.filelist if fi.orig_filename.endswith(event_file_ending)
        )
        for files, reader_func in (
            (entity_files, cls._import_entity),
            (event_files, cls._import_events),
        ):
            for file_info in files:
                with reader.open(file_info) as f:
                    full_name = splitext(file_info.orig_filename)[0]
                    print(f"Reading {reader.filename}:{full_name}...")
                    reader_func(f, full_name, company_id, user_id)

    @classmethod
    def _import_entity(cls, f: BinaryIO, full_name: str, company_id: str, user_id: str):
        module_name, _, class_name = full_name.rpartition(".")
        module = importlib.import_module(module_name)
        cls_: Type[mongoengine.Document] = getattr(module, class_name)
        print(f"Writing {cls_.__name__.lower()}s into database")
        for item in cls.json_lines(f):
            doc = cls_.from_json(item, created=True)
            if hasattr(doc, "user"):
                doc.user = user_id
            if hasattr(doc, "company"):
                doc.company = company_id
            if isinstance(doc, Project):
                cls_.objects(company=company_id, name=doc.name, id__ne=doc.id).update(
                    set__name=f"{doc.name}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}"
                )
            doc.save()
            if isinstance(doc, Task):
                cls.event_bll.delete_task_events(company_id, doc.id, allow_locked=True)

    @classmethod
    def _import_events(cls, f: BinaryIO, full_name: str, company_id: str, _):
        _, _, task_id = full_name[0 : -len(cls.events_file_suffix)].rpartition("_")
        print(f"Writing events for task {task_id} into database")
        for events_chunk in chunked_iter(cls.json_lines(f), 1000):
            events = [json.loads(item) for item in events_chunk]
            cls.event_bll.add_events(
                company_id, events=events, worker="", allow_locked_tasks=True
            )
