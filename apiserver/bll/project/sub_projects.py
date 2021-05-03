import itertools
from datetime import datetime
from typing import Tuple, Optional, Sequence, Mapping

from apiserver import database
from apiserver.apierrors import errors
from apiserver.config_repo import config
from apiserver.database.model.project import Project

name_separator = "/"
max_depth = config.get("services.projects.sub_projects.max_depth", 10)


def _validate_project_name(project_name: str) -> Tuple[str, str]:
    """
    Remove redundant '/' characters. Ensure that the project name is not empty
    and path to it is not larger then max_depth parameter.
    Return the cleaned up project name and location
    """
    name_parts = list(filter(None, project_name.split(name_separator)))
    if not name_parts:
        raise errors.bad_request.InvalidProjectName(name=project_name)

    if len(name_parts) > max_depth:
        raise errors.bad_request.ProjectPathExceedsMax(max_depth=max_depth)

    return name_separator.join(name_parts), name_separator.join(name_parts[:-1])


def _ensure_project(company: str, user: str, name: str) -> Optional[Project]:
    """
    Makes sure that the project with the given name exists
    If needed auto-create the project and all the missing projects in the path to it
    Return the project
    """
    name = name.strip(name_separator)
    if not name:
        return None

    project = _get_writable_project_from_name(company, name)
    if project:
        return project

    now = datetime.utcnow()
    name, location = _validate_project_name(name)
    project = Project(
        id=database.utils.id(),
        user=user,
        company=company,
        created=now,
        last_update=now,
        name=name,
        description="",
    )
    parent = _ensure_project(company, user, location)
    _save_under_parent(project=project, parent=parent)
    if parent:
        parent.update(last_update=now)

    return project


def _save_under_parent(project: Project, parent: Optional[Project]):
    """
    Save the project under the given parent project or top level (parent=None)
    Check that the project location matches the parent name
    """
    location, _, _ = project.name.rpartition(name_separator)
    if not parent:
        if location:
            raise ValueError(
                f"Project location {location} does not match empty parent name"
            )
        project.parent = None
        project.path = []
        project.save()
        return

    if location != parent.name:
        raise ValueError(
            f"Project location {location} does not match parent name {parent.name}"
        )
    project.parent = parent.id
    project.path = [*(parent.path or []), parent.id]
    project.save()


def _get_writable_project_from_name(
    company,
    name,
    _only: Optional[Sequence[str]] = ("id", "name", "path", "company", "parent"),
) -> Optional[Project]:
    """
    Return a project from name. If the project not found then return None
    """
    qs = Project.objects(company=company, name=name)
    if _only:
        qs = qs.only(*_only)
    return qs.first()


def _get_sub_projects(
    project_ids: Sequence[str], _only: Sequence[str] = ("id", "path")
) -> Mapping[str, Sequence[Project]]:
    """
    Return the list of child projects of all the levels for the parent project ids
    """
    qs = Project.objects(path__in=project_ids)
    if _only:
        _only = set(_only) | {"path"}
        qs = qs.only(*_only)
    subprojects = list(qs)

    return {
        pid: [s for s in subprojects if pid in (s.path or [])] for pid in project_ids
    }


def _ids_with_parents(project_ids: Sequence[str]) -> Sequence[str]:
    """
    Return project ids with all the parent projects
    """
    projects = Project.objects(id__in=project_ids).only("id", "path")
    parent_ids = set(itertools.chain.from_iterable(p.path for p in projects if p.path))
    return list({*(p.id for p in projects), *parent_ids})


def _ids_with_children(project_ids: Sequence[str]) -> Sequence[str]:
    """
    Return project ids with the ids of all the subprojects
    """
    subprojects = Project.objects(path__in=project_ids).only("id")
    return list({*project_ids, *(child.id for child in subprojects)})


def _update_subproject_names(
    project: Project,
    old_name: str,
    update_path: bool = False,
    old_path: Sequence[str] = None,
) -> int:
    """
    Update sub project names when the base project name changes
    Optionally update the paths
    """
    child_projects = _get_sub_projects(project_ids=[project.id], _only=("id", "name"))
    updated = 0
    for child in child_projects[project.id]:
        child_suffix = name_separator.join(
            child.name.split(name_separator)[len(old_name.split(name_separator)) :]
        )
        updates = {"name": name_separator.join((project.name, child_suffix))}
        if update_path:
            updates["path"] = project.path + child.path[len(old_path) :]
        updated += child.update(upsert=False, **updates)

    return updated


def _reposition_project_with_children(project: Project, parent: Project) -> int:
    new_location = parent.name if parent else None
    old_name = project.name
    old_path = project.path
    project.name = name_separator.join(
        filter(None, (new_location, project.name.split(name_separator)[-1]))
    )
    _save_under_parent(project, parent=parent)

    moved = 1 + _update_subproject_names(
        project=project, old_name=old_name, update_path=True, old_path=old_path
    )
    return moved
