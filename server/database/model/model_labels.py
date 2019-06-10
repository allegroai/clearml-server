from mongoengine import MapField, IntField


class ModelLabels(MapField):
    def __init__(self, *args, **kwargs):
        super(ModelLabels, self).__init__(field=IntField(), *args, **kwargs)

    def validate(self, value):
        super(ModelLabels, self).validate(value)
        if value and (len(set(value.values())) < len(value)):
            self.error("Same label id appears more than once in model labels")
