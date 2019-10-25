from config import config
from tests.automated import TestService


log = config.logger(__file__)


class TestTasksEdit(TestService):
    def new_task(self, **kwargs):
        return self.create_temp(
            "tasks", type="testing", name="test", input=dict(view=dict()), **kwargs
        )

    def new_model(self):
        return self.create_temp("models", name="test", uri="file:///a/b", labels={})

    def test_edit_model_ready(self):
        task = self.new_task()
        model = self.new_model()
        self.api.tasks.edit(task=task, execution=dict(model=model))

    def test_edit_model_not_ready(self):
        task = self.new_task()
        model = self.new_model()
        self.api.models.edit(model=model, ready=False)
        self.assertFalse(self.api.models.get_by_id(model=model).model.ready)
        self.api.tasks.edit(task=task, execution=dict(model=model))

    def test_edit_had_model_model_not_ready(self):
        ready_model = self.new_model()
        self.assert_(self.api.models.get_by_id(model=ready_model).model.ready)
        task = self.new_task(execution=dict(model=ready_model))
        not_ready_model = self.new_model()
        self.api.models.edit(model=not_ready_model, ready=False)
        self.assertFalse(self.api.models.get_by_id(model=not_ready_model).model.ready)
        self.api.tasks.edit(task=task, execution=dict(model=not_ready_model))
