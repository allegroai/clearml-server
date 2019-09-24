from mongoengine import EmbeddedDocument, StringField, DynamicField


class MetricEvent(EmbeddedDocument):
    meta = {
        # For backwards compatibility reasons
        'strict': False,
    }

    metric = StringField(required=True)
    variant = StringField(required=True)
    value = DynamicField(required=True)
    min_value = DynamicField()  # for backwards compatibility reasons
    max_value = DynamicField()  # for backwards compatibility reasons
