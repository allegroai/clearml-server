from jsonmodels import models, fields


class ProjectReq(models.Base):
    project = fields.StringField()


class GetHyperParamReq(ProjectReq):
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class GetHyperParamResp(models.Base):
    parameters = fields.ListField(str)
    remaining = fields.IntField()
    total = fields.IntField()
