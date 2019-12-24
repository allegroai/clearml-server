from config import config
from tests.automated import TestService


log = config.logger(__file__)


class TestTasksEdit(TestService):
    def setUp(self, **kwargs):
        super().setUp(version=2.5)

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

    def test_clone_task(self):
        script = dict(
            binary="python",
            requirements=dict(pip=["six"]),
            repository="https://example.come/foo/bar",
            entry_point="test.py",
            diff="foo",
        )
        execution = dict(parameters=dict(test="Test"))
        tags = ["hello"]
        system_tags = ["development", "test"]
        task = self.new_task(
            script=script, execution=execution, tags=tags, system_tags=system_tags
        )

        new_name = "new test"
        new_tags = ["by"]
        execution_overrides = dict(framework="Caffe")
        new_task_id = self.api.tasks.clone(
            task=task,
            new_task_name=new_name,
            new_task_tags=new_tags,
            execution_overrides=execution_overrides,
            new_task_parent=task,
        ).id
        new_task = self.api.tasks.get_by_id(task=new_task_id).task
        self.assertEqual(new_task.name, new_name)
        self.assertEqual(new_task.type, "testing")
        self.assertEqual(new_task.tags, new_tags)
        self.assertEqual(new_task.status, "created")
        self.assertEqual(new_task.script, script)
        self.assertEqual(new_task.parent, task)
        self.assertEqual(new_task.execution.parameters, execution["parameters"])
        self.assertEqual(new_task.execution.framework, execution_overrides["framework"])
        self.assertEqual(new_task.system_tags, [])
