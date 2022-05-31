from typing import Sequence

import attr
from mongoengine import Q

from apiserver.apierrors import errors
from apiserver.apierrors.errors.bad_request import InvalidProjectId
from apiserver.apimodels.base import UpdateResponse, MakePublicRequest, IdResponse
from apiserver.apimodels.projects import (
    GetParamsRequest,
    ProjectTagsRequest,
    ProjectTaskParentsRequest,
    ProjectHyperparamValuesRequest,
    ProjectsGetRequest,
    DeleteRequest,
    MoveRequest,
    MergeRequest,
    ProjectOrNoneRequest,
    ProjectRequest,
    ProjectModelMetadataValuesRequest,
)
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL, ProjectQueries
from apiserver.bll.project.project_cleanup import (
    delete_project,
    validate_project_delete,
)
from apiserver.database.errors import translate_errors_context
from apiserver.database.model import EntityVisibility
from apiserver.database.model.project import Project
from apiserver.database.utils import (
    parse_from_call,
    get_company_or_none_constraint,
)
from apiserver.service_repo import APICall, endpoint
from apiserver.services.utils import (
    conform_tag_fields,
    conform_output_tags,
    get_tags_filter_dictionary,
    sort_tags_response,
)
from apiserver.timing_context import TimingContext

org_bll = OrgBLL()
project_bll = ProjectBLL()
project_queries = ProjectQueries()

create_fields = {
    "name": None,
    "description": None,
    "tags": list,
    "system_tags": list,
    "default_output_destination": None,
}


@endpoint("projects.get_by_id", required_fields=["project"])
def get_by_id(call):
    assert isinstance(call, APICall)
    project_id = call.data["project"]

    with translate_errors_context():
        with TimingContext("mongo", "projects_by_id"):
            query = Q(id=project_id) & get_company_or_none_constraint(
                call.identity.company
            )
            project = Project.objects(query).first()
        if not project:
            raise errors.bad_request.InvalidProjectId(id=project_id)

        project_dict = project.to_proper_dict()
        conform_output_tags(call, project_dict)

        call.result.data = {"project": project_dict}


def _hidden_query(search_hidden: bool, ids: Sequence) -> Q:
    """
    1. Add only non-hidden tasks search condition (unless specifically specified differently)
    """
    if search_hidden or ids:
        return Q()

    return Q(system_tags__ne=EntityVisibility.hidden.value)


def _adjust_search_parameters(data: dict, shallow_search: bool):
    """
    1. Make sure that there is no external query on path
    2. If not shallow_search and parent is provided then parent can be at any place in path
    3. If shallow_search and no parent provided then use a top level parent
    """
    data.pop("path", None)
    if not shallow_search:
        if "parent" in data:
            data["path"] = data.pop("parent")
        return

    if "parent" not in data:
        data["parent"] = [None]


@endpoint("projects.get_all_ex", request_data_model=ProjectsGetRequest)
def get_all_ex(call: APICall, company_id: str, request: ProjectsGetRequest):
    data = call.data
    conform_tag_fields(call, data)
    allow_public = not request.non_public
    requested_ids = data.get("id")
    _adjust_search_parameters(
        data, shallow_search=request.shallow_search,
    )
    with TimingContext("mongo", "projects_get_all"):
        if request.active_users:
            ids = project_bll.get_projects_with_active_user(
                company=company_id,
                users=request.active_users,
                project_ids=requested_ids,
                allow_public=allow_public,
            )
            if not ids:
                return {"projects": []}
            data["id"] = ids

        ret_params = {}
        projects: Sequence[dict] = Project.get_many_with_join(
            company=company_id,
            query_dict=data,
            query=_hidden_query(search_hidden=request.search_hidden, ids=requested_ids),
            allow_public=allow_public,
            ret_params=ret_params,
        )

        if request.check_own_contents and requested_ids:
            existing_requested_ids = {
                project["id"] for project in projects if project["id"] in requested_ids
            }
            if existing_requested_ids:
                contents = project_bll.calc_own_contents(
                    company=company_id,
                    project_ids=list(existing_requested_ids),
                    filter_=request.include_stats_filter,
                )
                for project in projects:
                    project.update(**contents.get(project["id"], {}))

        conform_output_tags(call, projects)
        if not request.include_stats:
            call.result.data = {"projects": projects, **ret_params}
            return

        project_ids = {project["id"] for project in projects}
        stats, children = project_bll.get_project_stats(
            company=company_id,
            project_ids=list(project_ids),
            specific_state=request.stats_for_state,
            include_children=request.stats_with_children,
            search_hidden=request.search_hidden,
            filter_=request.include_stats_filter,
        )

        for project in projects:
            project["stats"] = stats[project["id"]]
            project["sub_projects"] = children[project["id"]]

        call.result.data = {"projects": projects, **ret_params}


@endpoint("projects.get_all")
def get_all(call: APICall):
    data = call.data
    conform_tag_fields(call, data)
    _adjust_search_parameters(
        data, shallow_search=data.get("shallow_search", False),
    )
    with TimingContext("mongo", "projects_get_all"):
        ret_params = {}
        projects = Project.get_many(
            company=call.identity.company,
            query_dict=data,
            query=_hidden_query(
                search_hidden=data.get("search_hidden"), ids=data.get("id")
            ),
            parameters=data,
            allow_public=True,
            ret_params=ret_params,
        )
        conform_output_tags(call, projects)
        call.result.data = {"projects": projects, **ret_params}


@endpoint(
    "projects.create", required_fields=["name"], response_data_model=IdResponse,
)
def create(call: APICall):
    identity = call.identity

    with translate_errors_context():
        fields = parse_from_call(call.data, create_fields, Project.get_fields())
        conform_tag_fields(call, fields, validate=True)

        return IdResponse(
            id=ProjectBLL.create(
                user=identity.user, company=identity.company, **fields,
            )
        )


@endpoint(
    "projects.update", required_fields=["project"], response_data_model=UpdateResponse
)
def update(call: APICall):
    """
    update

    :summary: Update project information.
              See `project.create` for parameters.
    :return: updated - `int` - number of projects updated
             fields - `[string]` - updated fields
    """
    fields = parse_from_call(
        call.data, create_fields, Project.get_fields(), discard_none_values=False
    )
    conform_tag_fields(call, fields, validate=True)
    updated = ProjectBLL.update(
        company=call.identity.company, project_id=call.data["project"], **fields
    )
    conform_output_tags(call, fields)
    call.result.data_model = UpdateResponse(updated=updated, fields=fields)


def _reset_cached_tags(company: str, projects: Sequence[str]):
    org_bll.reset_tags(company, Tags.Task, projects=projects)
    org_bll.reset_tags(company, Tags.Model, projects=projects)


@endpoint("projects.move", request_data_model=MoveRequest)
def move(call: APICall, company: str, request: MoveRequest):
    moved, affected_projects = ProjectBLL.move_project(
        company=company,
        user=call.identity.user,
        project_id=request.project,
        new_location=request.new_location,
    )
    _reset_cached_tags(company, projects=list(affected_projects))

    call.result.data = {"moved": moved}


@endpoint("projects.merge", request_data_model=MergeRequest)
def merge(call: APICall, company: str, request: MergeRequest):
    moved_entitites, moved_projects, affected_projects = ProjectBLL.merge_project(
        company, source_id=request.project, destination_id=request.destination_project
    )

    _reset_cached_tags(company, projects=list(affected_projects))

    call.result.data = {
        "moved_entities": moved_entitites,
        "moved_projects": moved_projects,
    }


@endpoint("projects.validate_delete")
def validate_delete(call: APICall, company_id: str, request: ProjectRequest):
    call.result.data = validate_project_delete(
        company=company_id, project_id=request.project
    )


@endpoint("projects.delete", request_data_model=DeleteRequest)
def delete(call: APICall, company_id: str, request: DeleteRequest):
    res, affected_projects = delete_project(
        company=company_id,
        project_id=request.project,
        force=request.force,
        delete_contents=request.delete_contents,
    )
    _reset_cached_tags(company_id, projects=list(affected_projects))
    call.result.data = {**attr.asdict(res)}


@endpoint(
    "projects.get_unique_metric_variants", request_data_model=ProjectOrNoneRequest
)
def get_unique_metric_variants(
    call: APICall, company_id: str, request: ProjectOrNoneRequest
):

    metrics = project_queries.get_unique_metric_variants(
        company_id,
        [request.project] if request.project else None,
        include_subprojects=request.include_subprojects,
    )

    call.result.data = {"metrics": metrics}


@endpoint("projects.get_model_metadata_keys",)
def get_model_metadata_keys(call: APICall, company_id: str, request: GetParamsRequest):
    total, remaining, keys = project_queries.get_model_metadata_keys(
        company_id,
        project_ids=[request.project] if request.project else None,
        include_subprojects=request.include_subprojects,
        page=request.page,
        page_size=request.page_size,
    )

    call.result.data = {
        "total": total,
        "remaining": remaining,
        "keys": keys,
    }


@endpoint("projects.get_model_metadata_values")
def get_model_metadata_values(
    call: APICall, company_id: str, request: ProjectModelMetadataValuesRequest
):
    total, values = project_queries.get_model_metadata_distinct_values(
        company_id,
        project_ids=request.projects,
        key=request.key,
        include_subprojects=request.include_subprojects,
        allow_public=request.allow_public,
    )
    call.result.data = {
        "total": total,
        "values": values,
    }


@endpoint(
    "projects.get_hyper_parameters",
    min_version="2.9",
    request_data_model=GetParamsRequest,
)
def get_hyper_parameters(call: APICall, company_id: str, request: GetParamsRequest):

    total, remaining, parameters = project_queries.get_aggregated_project_parameters(
        company_id,
        project_ids=[request.project] if request.project else None,
        include_subprojects=request.include_subprojects,
        page=request.page,
        page_size=request.page_size,
    )

    call.result.data = {
        "total": total,
        "remaining": remaining,
        "parameters": parameters,
    }


@endpoint(
    "projects.get_hyperparam_values",
    min_version="2.13",
    request_data_model=ProjectHyperparamValuesRequest,
)
def get_hyperparam_values(
    call: APICall, company_id: str, request: ProjectHyperparamValuesRequest
):
    total, values = project_queries.get_task_hyperparam_distinct_values(
        company_id,
        project_ids=request.projects,
        section=request.section,
        name=request.name,
        include_subprojects=request.include_subprojects,
        allow_public=request.allow_public,
    )
    call.result.data = {
        "total": total,
        "values": values,
    }


@endpoint("projects.get_project_tags")
def get_tags(call: APICall, company, request: ProjectTagsRequest):
    tags, system_tags = project_bll.get_project_tags(
        company,
        include_system=request.include_system,
        filter_=get_tags_filter_dictionary(request.filter),
        projects=request.projects,
    )
    call.result.data = sort_tags_response({"tags": tags, "system_tags": system_tags})


@endpoint(
    "projects.get_task_tags", min_version="2.8", request_data_model=ProjectTagsRequest
)
def get_tags(call: APICall, company, request: ProjectTagsRequest):
    ret = org_bll.get_tags(
        company,
        Tags.Task,
        include_system=request.include_system,
        filter_=get_tags_filter_dictionary(request.filter),
        projects=request.projects,
    )
    call.result.data = sort_tags_response(ret)


@endpoint(
    "projects.get_model_tags", min_version="2.8", request_data_model=ProjectTagsRequest
)
def get_tags(call: APICall, company, request: ProjectTagsRequest):
    ret = org_bll.get_tags(
        company,
        Tags.Model,
        include_system=request.include_system,
        filter_=get_tags_filter_dictionary(request.filter),
        projects=request.projects,
    )
    call.result.data = sort_tags_response(ret)


@endpoint(
    "projects.make_public", min_version="2.9", request_data_model=MakePublicRequest
)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    call.result.data = Project.set_public(
        company_id, ids=request.ids, invalid_cls=InvalidProjectId, enabled=True
    )


@endpoint(
    "projects.make_private", min_version="2.9", request_data_model=MakePublicRequest
)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    call.result.data = Project.set_public(
        company_id, ids=request.ids, invalid_cls=InvalidProjectId, enabled=False
    )


@endpoint(
    "projects.get_task_parents",
    min_version="2.12",
    request_data_model=ProjectTaskParentsRequest,
)
def get_task_parents(
    call: APICall, company_id: str, request: ProjectTaskParentsRequest
):
    call.result.data = {
        "parents": project_bll.get_task_parents(
            company_id,
            projects=request.projects,
            include_subprojects=request.include_subprojects,
            state=request.tasks_state,
        )
    }
