from enum import Enum

from mongoengine import Document, StringField

from apiserver.apierrors import errors
from apiserver.database.model.base import DbModelMixin, ABSTRACT_FLAG
from apiserver.database.model.company import Company
from apiserver.database.model.user import User


class AttributedDocument(DbModelMixin, Document):
    """
    Represents objects which are attributed to a company and a user or to "no one".
    Company must be required since it can be used as unique field.
    """
    meta = ABSTRACT_FLAG
    company = StringField(required=True, reference_field=Company)
    user = StringField(reference_field=User)

    def is_public(self) -> bool:
        return bool(self.company)


class PrivateDocument(AttributedDocument):
    """
    Represents documents which always belong to a single company
    """
    meta = ABSTRACT_FLAG
    # can not have an empty string as this is the "public" marker
    company = StringField(required=True, reference_field=Company, min_length=1)
    user = StringField(reference_field=User, required=True)

    def is_public(self) -> bool:
        return False


def validate_id(cls, company, **kwargs):
    """
    Validate existence of objects with certain IDs. within company.
    :param cls: Model class to search in
    :param company: Company to search in
    :param kwargs: Mapping of field name to object ID. If any ID does not have a corresponding object,
                    it will be reported along with the name it was assigned to.
    :return:
    """
    ids = set(kwargs.values())
    objs = list(cls.objects(company=company, id__in=ids).only('id'))
    missing = ids - set(x.id for x in objs)
    if not missing:
        return
    id_to_name = {}
    for name, obj_id in kwargs.items():
        id_to_name.setdefault(obj_id, []).append(name)
    raise errors.bad_request.ValidationError(
        'Invalid {} ids'.format(cls.__name__.lower()),
        **{name: obj_id for obj_id in missing for name in id_to_name[obj_id]}
    )


class EntityVisibility(Enum):
    active = "active"
    archived = "archived"
    hidden = "hidden"
