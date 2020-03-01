from typing import Optional, TypeVar, Generic, Type

from redis import StrictRedis

from timing_context import TimingContext

T = TypeVar("T")


class RedisCacheManager(Generic[T]):
    """
    Class for store/retreive of state objects from redis

    self.state_class - class of the state
    self.redis - instance of redis
    self.expiration_interval - expiration interval in seconds
    """

    def __init__(
        self, state_class: Type[T], redis: StrictRedis, expiration_interval: int
    ):
        self.state_class = state_class
        self.redis = redis
        self.expiration_interval = expiration_interval

    def set_state(self, state: T) -> None:
        redis_key = self._get_redis_key(state.id)
        with TimingContext("redis", "cache_set_state"):
            self.redis.set(redis_key, state.to_json())
            self.redis.expire(redis_key, self.expiration_interval)

    def get_state(self, state_id) -> Optional[T]:
        redis_key = self._get_redis_key(state_id)
        with TimingContext("redis", "cache_get_state"):
            response = self.redis.get(redis_key)
        if response:
            return self.state_class.from_json(response)

    def delete_state(self, state_id) -> None:
        with TimingContext("redis", "cache_delete_state"):
            self.redis.delete(self._get_redis_key(state_id))

    def _get_redis_key(self, state_id):
        return f"{self.state_class}/{state_id}"
