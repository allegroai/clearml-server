from typing import Sequence

from jsonmodels import validators
from jsonmodels.fields import StringField, BoolField
from jsonmodels.models import Base

from apiserver.apimodels import ListField


class MetadataItem(Base):
    key = StringField(required=True)
    type = StringField(required=True)
    value = StringField(required=True)


class DeleteMetadata(Base):
    keys: Sequence[str] = ListField(str, validators=validators.Length(minimum_value=1))


class AddOrUpdateMetadata(Base):
    metadata: Sequence[MetadataItem] = ListField(
        [MetadataItem], validators=validators.Length(minimum_value=1)
    )
    replace_metadata = BoolField(default=False)
