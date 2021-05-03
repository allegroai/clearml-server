from typing import Sequence

from jsonmodels.models import Base
from jsonmodels.validators import Length

from apiserver.apimodels import ListField, IntField


class BatchRequest(Base):
    ids: Sequence[str] = ListField([str], validators=Length(minimum_value=1))


class BatchResponse(Base):
    succeeded: int = IntField()
    failures: Sequence[dict] = ListField([dict])
