from mongoengine import EmbeddedDocument, StringField, DateTimeField, LongField, DynamicField


class MetricEvent(EmbeddedDocument):
    metric = StringField(required=True, )
    variant = StringField(required=True)
    type = StringField(required=True)
    timestamp = DateTimeField(default=0, required=True)
    iter = LongField()
    value = DynamicField(required=True)

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**{k: v for k, v in kwargs.items() if k in cls._fields})
