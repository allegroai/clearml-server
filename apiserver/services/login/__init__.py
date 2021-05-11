from apiserver.apimodels.login import (
    GetSupportedModesRequest,
    GetSupportedModesResponse,
    BasicMode,
    BasicGuestMode,
    ServerErrors,
)
from apiserver.config import info
from apiserver.service_repo import endpoint, APICall
from apiserver.service_repo.auth.fixed_user import FixedUser


@endpoint("login.supported_modes", response_data_model=GetSupportedModesResponse)
def supported_modes(call: APICall, _, __: GetSupportedModesRequest):
    guest_user = FixedUser.get_guest_user()
    if guest_user:
        guest = BasicGuestMode(
            enabled=True,
            name=guest_user.name,
            username=guest_user.username,
            password=guest_user.password,
        )
    else:
        guest = BasicGuestMode()

    return GetSupportedModesResponse(
        basic=BasicMode(enabled=FixedUser.enabled(), guest=guest),
        sso={},
        sso_providers=[],
        server_errors=ServerErrors(
            missed_es_upgrade=info.missed_es_upgrade,
            es_connection_error=info.es_connection_error,
        ),
        authenticated=call.auth is not None,
    )


@endpoint("login.logout", min_version="2.13")
def logout(call: APICall, _, __):
    call.result.set_auth_cookie(None)
