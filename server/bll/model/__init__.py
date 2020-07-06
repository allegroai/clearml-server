from typing import Optional, Sequence

from mongoengine import Q

from database.model.model import Model
from database.utils import get_company_or_none_constraint


class ModelBLL:
    def get_frameworks(self, company, project_ids: Optional[Sequence]) -> Sequence:
        """
        Return the list of unique frameworks used by company and public models
        If project ids passed then only models from these projects are considered
        """
        query = get_company_or_none_constraint(company)
        if project_ids:
            query &= Q(project__in=project_ids)
        return Model.objects(query).distinct(field="framework")
