class APIError(Exception):
    def __init__(self, msg, code=500, subcode=0, **_):
        super(APIError, self).__init__()
        self._msg = msg
        self._code = code
        self._subcode = subcode

    @property
    def msg(self):
        return self._msg

    @property
    def code(self):
        return self._code

    @property
    def subcode(self):
        return self._subcode

    def __str__(self):
        return self.msg
