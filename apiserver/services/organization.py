from collections import defaultdict
from operator import itemgetter

from apiserver.apimodels.organization import TagsRequest
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.database.model import User
from apiserver.service_repo import endpoint, APICall
from apiserver.services.utils import get_tags_filter_dictionary, sort_tags_response

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

    call.result.data = sort_tags_response(ret)


@endpoint("organization.get_user_companies")
def get_user_companies(call: APICall, company_id: str, _):
    users = [
        {"id": u.id, "name": u.name, "avatar": u.avatar}
        for u in User.objects(company=company_id).only("avatar", "name", "company")
    ]

    call.result.data = {
        "companies": [
            {
                "id": company_id,
                "name": call.identity.company_name,
                "allocated": len(users),
                "owners": sorted(users, key=itemgetter("name")),
            }
        ]
    }
