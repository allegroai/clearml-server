from typing import Optional, Sequence, Iterable, Union

from apiserver.config_repo import config

log = config.logger(__file__)

RANGE_IGNORE_VALUE = -1


class Builder:
    @staticmethod
    def dates_range(from_date: Union[int, float], to_date: Union[int, float]) -> dict:
        return {
            "range": {
                "timestamp": {
                    "gte": int(from_date),
                    "lte": int(to_date),
                    "format": "epoch_second",
                }
            }
        }

    @staticmethod
    def terms(field: str, values: Iterable[str]) -> dict:
        return {"terms": {field: list(values)}}

    @staticmethod
    def normalize_range(
        range_: Sequence[Union[int, float]],
        ignore_value: Union[int, float] = RANGE_IGNORE_VALUE,
    ) -> Optional[Sequence[Union[int, float]]]:
        if not range_ or set(range_) == {ignore_value}:
            return None
        if len(range_) < 2:
            return [range_[0]] * 2
        return range_
