from parameterized import parameterized

from config import config
from tests.automated import TestService

log = config.logger(__file__)


continuations = (
    (lambda self, task: self.tasks.reset(task=task),),
    (lambda self, task: self.tasks.delete(task=task),),
)


def reset_and_delete():
    """
    Parametrize a test for both delete and reset operations,
    which should yield the same results.
    NOTE: "parameterized" engages in call stack manipulation,
        so be careful when changing the application of the decorator.
        For example, receiving "func" as a parameter and passing it to
        "expand" doesn't work.
    """
    return parameterized.expand(
        [
            (lambda self, task: self.tasks.delete(task=task),),
            (lambda self, task: self.tasks.reset(task=task),),
        ],
        name_func=lambda func, num, _: "{}_{}".format(
            func.__name__, ["delete", "reset"][int(num)]
        ),
    )


class TestTasksResetDelete(TestService):

    TASK_CANNOT_BE_DELETED_CODES = (400, 123)

    def setUp(self, version="1.7"):
        super(TestTasksResetDelete, self).setUp(version=version)
        self.tasks = self.api.tasks
        self.models = self.api.models

    def new_task(self, **kwargs):
        task_id = self.tasks.create(
            type='testing', name='server-test', input=dict(view=dict()), **kwargs
        )['id']
        self.defer(self.tasks.delete, can_fail=True, task=task_id, force=True)
        return task_id

    def new_model(self, **kwargs):
        model_id = self.models.create(name='test', uri='file:///a', labels={}, **kwargs)['id']
        self.defer(self.models.delete, can_fail=True, model=model_id, force=True)
        return model_id

    def delete_failure(self):
        return self.api.raises(self.TASK_CANNOT_BE_DELETED_CODES)

    def publish_created_task(self, task_id):
        self.tasks.started(task=task_id)
        self.tasks.stopped(task=task_id)
        self.tasks.publish(task=task_id)

    @reset_and_delete()
    def test_plain(self, cont):
        cont(self, self.new_task())

    @reset_and_delete()
    def test_draft_child(self, cont):
        parent = self.new_task()
        self.new_task(parent=parent)
        cont(self, parent)

    @reset_and_delete()
    def test_published_child(self, cont):
        parent = self.new_task()
        child = self.new_task(parent=parent)
        self.publish_created_task(child)
        with self.delete_failure():
            cont(self, parent)

    @reset_and_delete()
    def test_draft_model(self, cont):
        task = self.new_task()
        model = self.new_model()
        self.models.edit(model=model, task=task, ready=False)
        cont(self, task)

    @reset_and_delete()
    def test_published_model(self, cont):
        task = self.new_task()
        model = self.new_model()
        self.models.edit(model=model, task=task, ready=True)
        with self.delete_failure():
            cont(self, task)
