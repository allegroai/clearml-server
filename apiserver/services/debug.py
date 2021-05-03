from apiserver.service_repo import APICall, endpoint


@endpoint("debug.ping")
def ping(call: APICall, _, __):
    call.result.data = {"msg": "ClearML server"}
