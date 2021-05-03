from enum import Enum


class StringEnum(str, Enum):
    def __str__(self):
        return self.value

    @classmethod
    def values(cls):
        return list(map(str, cls))

    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name
