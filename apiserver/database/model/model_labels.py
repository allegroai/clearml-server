from apiserver.database.fields import NoneType, UnionField, SafeMapField


class ModelLabels(SafeMapField):
    def __init__(self, *args, **kwargs):
        super(ModelLabels, self).__init__(
            field=UnionField(types=(int, NoneType)), *args, **kwargs
        )

    def validate(self, value):
        super(ModelLabels, self).validate(value)
        non_empty_values = list(filter(None, value.values()))
        if non_empty_values and len(set(non_empty_values)) < len(non_empty_values):
            self.error("Same label id appears more than once in model labels")
