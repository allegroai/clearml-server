from contextlib import contextmanager
from typing import Optional, TypeVar, Generic, Type, Callable

from redis import StrictRedis

from apiserver import database

T = TypeVar("T")


def _do_nothing(_: T):
    return


class RedisCacheManager(Generic[T]):
    """
    Class for store/retrieve of state objects from redis

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
        self.redis.set(redis_key, state.to_json())
        self.redis.expire(redis_key, self.expiration_interval)

    def get_state(self, state_id) -> Optional[T]:
        redis_key = self._get_redis_key(state_id)
        response = self.redis.get(redis_key)
        if response:
            return self.state_class.from_json(response)

    def delete_state(self, state_id) -> None:
        self.redis.delete(self._get_redis_key(state_id))

    def _get_redis_key(self, state_id):
        return f"{self.state_class}/{state_id}"

    def get_or_create_state_core(
        self,
        state_id=None,
        init_state: Callable[[T], None] = _do_nothing,
        validate_state: Callable[[T], None] = _do_nothing,
    ) -> T:
        state = self.get_state(state_id) if state_id else None
        if state:
            validate_state(state)
        else:
            state = self.state_class(id=database.utils.id())
            init_state(state)

        return state

    @contextmanager
    def get_or_create_state(
        self,
        state_id=None,
        init_state: Callable[[T], None] = _do_nothing,
        validate_state: Callable[[T], None] = _do_nothing,
    ):
        """
        Try to retrieve state with the given id from the Redis cache if yes then validates it
        If no then create a new one with randomly generated id
        Yield the state and write it back to redis once the user code block exits
        :param state_id: id of the state to retrieve
        :param init_state: user callback to init the newly created state
        If not passed then no init except for the id generation is done
        :param validate_state: user callback to validate the state if retrieved from cache
        Should throw an exception if the state is not valid. If not passed then no validation is done
        """
        state = self.get_or_create_state_core(
            state_id=state_id, init_state=init_state, validate_state=validate_state
        )

        try:
            yield state
        finally:
            self.set_state(state)
