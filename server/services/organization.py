from apimodels.organization import TagsRequest
from bll.organization import OrgBLL
from service_repo import endpoint, APICall

org_bll = OrgBLL()


@endpoint("organization.get_tags", request_data_model=TagsRequest)
def get_tags(call: APICall, company, request: TagsRequest):
    filter_ = request.filter.system_tags if request.filter else None
    call.result.data = org_bll.get_tags(
        company, include_system=request.include_system, filter_=filter_
    )
