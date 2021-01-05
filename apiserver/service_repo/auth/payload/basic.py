from .payload import Payload
from .auth_type import AuthType


class Basic(Payload):
    def __init__(self, user_key, identity=None, entities=None, **_):
        super(Basic, self).__init__(
            AuthType.basic, identity=identity, entities=entities)
        self._user_key = user_key

    @property
    def user_key(self):
        return self._user_key

    def get_log_entry(self):
        d = super(Basic, self).get_log_entry()
        d.update(user_key=self.user_key)
        return d
