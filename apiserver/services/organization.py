from collections import defaultdict

from apiserver.apimodels.organization import TagsRequest
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.service_repo import endpoint, APICall
from apiserver.services.utils import get_tags_filter_dictionary, get_tags_response

org_bll = OrgBLL()


@endpoint("organization.get_tags", request_data_model=TagsRequest)
def get_tags(call: APICall, company, request: TagsRequest):
    filter_dict = get_tags_filter_dictionary(request.filter)
    ret = defaultdict(set)
    for entity in Tags.Model, Tags.Task:
        tags = org_bll.get_tags(
            company, entity, include_system=request.include_system, filter_=filter_dict,
        )
        for field, vals in tags.items():
            ret[field] |= vals

    call.result.data = get_tags_response(ret)
