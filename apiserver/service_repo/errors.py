class PathParsingError(Exception):
    def __init__(self, msg):
        super(PathParsingError, self).__init__(msg)


class MalformedPathError(PathParsingError):
    pass


class InvalidVersionError(PathParsingError):
    pass


class CallParsingError(Exception):
    pass


class CallFailedError(Exception):
    pass
