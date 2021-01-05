from boltons.dictutils import OneToOne

from apierrors import errors


class ParameterKeyEscaper:
    """
    Makes the fields name ready for use with MongoDB and Mongoengine
    . and $ are replaced with their codes
    __ and leading _ are escaped
    Since % is used as an escape character the % is also escaped
    """

    _mapping = OneToOne({".": "%2E", "$": "%24", "__": "%_%_"})

    @classmethod
    def escape(cls, value):
        """ Quote a parameter key """
        if value is None:
            raise errors.bad_request.ValidationError("Key cannot be empty")

        value = value.strip().replace("%", "%%")

        for c, r in cls._mapping.items():
            value = value.replace(c, r)

        if value.startswith("_"):
            value = "%_" + value[1:]

        return value

    @classmethod
    def _unescape(cls, value):
        for c, r in cls._mapping.inv.items():
            value = value.replace(c, r)
        return value

    @classmethod
    def unescape(cls, value):
        """ Unquote a quoted parameter key """
        value = "%".join(map(cls._unescape, value.split("%%")))

        if value.startswith("%_"):
            value = "_" + value[2:]

        return value
