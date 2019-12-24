from mongoengine import (
    StringField,
    EmbeddedDocument,
    EmbeddedDocumentListField,
    EmailField,
    DateTimeField,
)

from database import Database, strict
from database.model import DbModelMixin
from database.model.base import AuthDocument
from database.utils import get_options


class Entities(object):
    company = "company"
    task = "task"
    user = "user"
    model = "model"


class Role(object):
    system = "system"
    """ Internal system component """
    root = "root"
    """ Root admin (person) """
    admin = "admin"
    """ Company administrator """
    superuser = "superuser"
    """ Company super user """
    user = "user"
    """ Company user """
    annotator = "annotator"
    """ Annotator with limited access"""

    @classmethod
    def get_system_roles(cls) -> set:
        return {cls.system, cls.root}

    @classmethod
    def get_company_roles(cls) -> set:
        return set(get_options(cls)) - cls.get_system_roles()


class Credentials(EmbeddedDocument):
    key = StringField(required=True)
    secret = StringField(required=True)
    last_used = DateTimeField()


class User(DbModelMixin, AuthDocument):
    meta = {"db_alias": Database.auth, "strict": strict}

    id = StringField(primary_key=True)
    name = StringField()

    created = DateTimeField()
    """ User auth entry creation time """

    validated = DateTimeField()
    """ Last validation (login) time """

    role = StringField(required=True, choices=get_options(Role), default=Role.user)
    """ User role """

    company = StringField(required=True)
    """ Company this user belongs to """

    credentials = EmbeddedDocumentListField(Credentials, default=list)
    """ Credentials generated for this user """

    email = EmailField(unique=True, required=True)
    """ Email uniquely identifying the user """
