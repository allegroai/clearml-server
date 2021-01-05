import attr


def typed_attrs(cls):
    """
    Created a type-validated attrs class.
    Attributes are set to their default values if they're assigned `None` to.
    """
    for attrib in cls.__dict__.values():
        try:
            if not attrib.type:
                continue
            validator = attrib._validator
        except AttributeError:
            continue
        instance_of = attr.validators.instance_of(attrib.type)
        attrib._validator = (
            attr.validators.and_(validator, instance_of) if validator else instance_of
        )
        if attrib._default != attr.NOTHING:

            def converter(default):
                """
                Create a converter that interprets `None` as "use default".
                Required in order to create a new lexical scope, see https://stackoverflow.com/a/233835
                """
                return lambda x: default if x is None else x

            attrib.converter = converter(attrib._default)

    return attr.s(cls)
