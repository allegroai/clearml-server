from enum import Enum


class StringEnum(Enum):
    def __str__(self):
        return self.value

    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name