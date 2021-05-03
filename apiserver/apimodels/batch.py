from typing import Sequence

from jsonmodels.fields import StringField
from jsonmodels.models import Base
from jsonmodels.validators import Length

from apiserver.apimodels import ListField
from apiserver.apimodels.base import UpdateResponse


class BatchRequest(Base):
    ids: Sequence[str] = ListField([str], validators=Length(minimum_value=1))


class BatchResponse(Base):
    succeeded: Sequence[dict] = ListField([dict])
    failed: Sequence[dict] = ListField([dict])


class UpdateBatchItem(UpdateResponse):
    id: str = StringField()


class UpdateBatchResponse(BatchResponse):
    succeeded: Sequence[UpdateBatchItem] = ListField(UpdateBatchItem)
