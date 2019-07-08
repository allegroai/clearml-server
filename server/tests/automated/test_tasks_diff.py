from config import config
from tests.automated import TestService

log = config.logger(__file__)


class TestTasksDiff(TestService):

    def setUp(self, version="2.0"):
        super(TestTasksDiff, self).setUp(version=version)

    def new_task(self, **kwargs):
        return self.create_temp(
            "tasks", name="test", type="testing", input=dict(view=dict()), **kwargs
        )

    def _compare_script(self, task, script):
        for key, value in script.items():
            self.assertEqual(task.script[key], value)

    def test_not_deleted(self):
        task_id = self.new_task()
        script = dict(
            requirements=dict(pip=["six"]),
            repository="https://example.come/foo/bar",
            entry_point="test.py",
            diff="foo",
        )
        self.api.tasks.edit(task=task_id, script=script)
        self.api.tasks.started(task=task_id)
        self.api.tasks.reset(task=task_id)
        task = self.api.tasks.get_by_id(task=task_id).task
        self._compare_script(task, script)
        new_reqs = dict()
        self.api.tasks.set_requirements(task=task_id, requirements=new_reqs)
        script["requirements"] = new_reqs
        task = self.api.tasks.get_by_id(task=task_id).task
        self._compare_script(task, script)
