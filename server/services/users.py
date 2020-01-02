from copy import deepcopy
from typing import Dict, Tuple

import dpath
from boltons.iterutils import remap
from mongoengine import Q

from apierrors import errors
from apimodels.base import UpdateResponse
from apimodels.users import CreateRequest, SetPreferencesRequest
from bll.user import UserBLL
from config import config
from database.errors import translate_errors_context
from database.model.auth import Role
from database.model.company import Company
from database.model.user import User
from database.utils import parse_from_call
from service_repo import APICall, endpoint
from utilities.json import loads, dumps

log = config.logger(__file__)
get_all_query_options = User.QueryParameterOptions(list_fields=("id",))


def get_user(call, user_id, only=None):
    """
    Get user object by the user's ID
    :param call: API call
    :param user_id: user ID
    :param only: fields to include in projection, by default all
    :return: User object
    """
    if call.identity.role in (Role.system,):
        # allow system users to get info for all users
        query = dict(id=user_id)
    else:
        query = dict(id=user_id, company=call.identity.company)

    with translate_errors_context("retrieving user"):
        user = User.objects(**query)
        if only:
            user = user.only(*only)
        res = user.first()
        if not res:
            raise errors.bad_request.InvalidUserId(**query)

        return res.to_proper_dict()


@endpoint("users.get_by_id", required_fields=["user"])
def get_by_id(call):
    assert isinstance(call, APICall)
    user_id = call.data["user"]
    call.result.data = {"user": get_user(call, user_id)}


@endpoint("users.get_all_ex", required_fields=[])
def get_all_ex(call):
    assert isinstance(call, APICall)

    with translate_errors_context("retrieving users"):
        res = User.get_many_with_join(
            company=call.identity.company,
            query_dict=call.data,
            query_options=get_all_query_options,
        )

        call.result.data = {"users": res}


@endpoint("users.get_all", required_fields=[])
def get_all(call):
    assert isinstance(call, APICall)

    with translate_errors_context("retrieving users"):
        res = User.get_many(
            company=call.identity.company,
            parameters=call.data,
            query_dict=call.data,
            query_options=get_all_query_options,
        )

        call.result.data = {"users": res}


@endpoint("users.get_current_user")
def get_current_user(call):
    assert isinstance(call, APICall)

    with translate_errors_context("retrieving users"):

        projection = (
            {"company.name"}
            .union(User.get_fields())
            .difference(User.get_exclude_fields())
        )
        res = User.get_many_with_join(
            query=Q(id=call.identity.user),
            company=call.identity.company,
            override_projection=projection,
        )

        if not res:
            raise errors.bad_request.InvalidUser("failed loading user")

        user = res[0]
        user["role"] = call.identity.role

        resp = {"user": user}
        call.result.data = resp


create_fields = {
    "name": None,
    "family_name": None,
    "given_name": None,
    "avatar": None,
    "company": Company,
    "preferences": dict,
}


@endpoint("users.create", request_data_model=CreateRequest)
def create(call: APICall):
    UserBLL.create(call.data_model)


@endpoint("users.delete", required_fields=["user"])
def delete(call):
    assert isinstance(call, APICall)
    UserBLL.delete(call.data["user"])


def update_user(user_id, company_id, data):
    # type: (str, str, Dict) -> Tuple[int, Dict]
    """
    Update user.
    :param user_id: user ID to update
    :param company_id: ID of company user belongs to
    :param data: mapping to update user by
    :return: (updated fields count, updated fields) pair
    """
    update_fields = {
        k: v for k, v in create_fields.items() if k in User.user_set_allowed()
    }
    partial_update_dict = parse_from_call(data, update_fields, User.get_fields())
    with translate_errors_context("updating user"):
        return User.safe_update(company_id, user_id, partial_update_dict)


@endpoint("users.update", required_fields=["user"], response_data_model=UpdateResponse)
def update(call, company_id, _):
    assert isinstance(call, APICall)
    user_id = call.data["user"]
    update_count, updated_fields = update_user(user_id, company_id, call.data)
    call.result.data_model = UpdateResponse(updated=update_count, fields=updated_fields)


def get_user_preferences(call):
    user_id = call.identity.user
    preferences = get_user(call, user_id, ["preferences"]).get("preferences")
    if preferences and isinstance(preferences, str):
        preferences = loads(preferences)
    return preferences or {}


@endpoint("users.get_preferences")
def get_preferences(call):
    assert isinstance(call, APICall)
    return {"preferences": get_user_preferences(call)}


@endpoint("users.set_preferences", request_data_model=SetPreferencesRequest)
def set_preferences(call, company_id, req_model):
    # type: (APICall, str, SetPreferencesRequest) -> Dict
    assert isinstance(call, APICall)
    changes = req_model.preferences

    def invalid_key(_, key, __):
        if not isinstance(key, str):
            return True
        elif key.startswith("$") or "." in key:
            raise errors.bad_request.FieldsValueError(
                f"Key {key} is invalid. Keys cannot start with '$' or contain '.'."
            )
        return True

    remap(changes, visit=invalid_key)

    base_preferences = get_user_preferences(call)
    new_preferences = deepcopy(base_preferences)
    for key, value in changes.items():
        try:
            dpath.new(new_preferences, key, value, separator=".")
        except Exception:
            log.exception(
                'invalid preferences update for user "{}": key=`%s`, value=`%s`',
                key,
                value,
            )
            raise errors.bad_request.InvalidPreferencesUpdate(key=key, value=value)

    if new_preferences == base_preferences:
        updated, fields = 0, {}
    else:
        with translate_errors_context("updating user preferences"):
            updated = User.objects(id=call.identity.user, company=company_id).update(
                upsert=False, preferences=dumps(new_preferences)
            )

    return {
        "updated": updated,
        "fields": {"preferences": new_preferences} if updated else {},
    }
