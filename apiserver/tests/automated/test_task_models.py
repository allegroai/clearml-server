from copy import deepcopy
from typing import Sequence, Optional

from packaging.version import parse

from apiserver.tests.automated import TestService


class TestTaskModels(TestService):
    def setUp(self, version="2.13"):
        super().setUp(version=version)

    def test_new_apis(self):
        # no models
        empty_task = self.new_task()
        self.assertModels(empty_task, [], [])

        id1, id2 = self.new_model("model1"), self.new_model("model2")
        input_models = [
            {"name": "input1", "model": id1},
            {"name": "input2", "model": id2},
        ]
        output_models = [
            {"name": "output1", "model": "id3"},
            {"name": "output2", "model": "id4"},
        ]

        # task creation with models
        task = self.new_task(models={"input": input_models, "output": output_models})
        self.assertModels(task, input_models, output_models)

        # add_or_update existing model
        res = self.api.tasks.add_or_update_model(
            task=task, name="input1", type="input", model="Test"
        )
        self.assertEqual(res.updated, 1)
        modified_input = deepcopy(input_models)
        modified_input[0]["model"] = "Test"
        self.assertModels(task, modified_input, output_models)

        # add_or_update new mode
        res = self.api.tasks.add_or_update_model(
            task=task, name="output3", type="output", model="TestOutput"
        )
        self.assertEqual(res.updated, 1)
        modified_output = deepcopy(output_models)
        modified_output.append({"name": "output3", "model": "TestOutput"})
        self.assertModels(task, modified_input, modified_output)

        # task editing
        self.api.tasks.edit(
            task=task, models={"input": input_models, "output": output_models}
        )
        self.assertModels(task, input_models, output_models)

        # delete models
        res = self.api.tasks.delete_models(
            task=task,
            models=[
                {"name": "input1", "type": "input"},
                {"name": "input2", "type": "input"},
                {"name": "output1", "type": "output"},
                {"name": "not_existing", "type": "output"},
            ]
        )
        self.assertEqual(res.updated, 1)
        self.assertModels(task, [], output_models[1:])

    def assertModels(
        self, task_id: str, input_models: Sequence[dict], output_models: Sequence[dict],
    ):
        def get_model_id(model: dict) -> Optional[str]:
            if not model:
                return None
            id_ = model.get("model")
            if isinstance(id_, str):
                return id_
            if id_ is None or id_ == {}:
                return None
            return id_.get("id")

        def compare_models(actual: Sequence[dict], expected: Sequence[dict]):
            self.assertEqual(
                [(m["name"], get_model_id(m)) for m in actual],
                [(m["name"], m["model"]) for m in expected],
            )

        for task in (
            self.api.tasks.get_all_ex(id=task_id).tasks[0],
            self.api.tasks.get_all(id=task_id).tasks[0],
            self.api.tasks.get_by_id(task=task_id).task,
        ):
            compare_models(task.models.input, input_models)
            compare_models(task.models.output, output_models)
            if self._version < parse("2.13"):
                self.assertEqual(
                    get_model_id(task.execution),
                    input_models[0]["model"] if input_models else None,
                )
                self.assertEqual(
                    get_model_id(task.output),
                    output_models[-1]["model"] if output_models else None,
                )

    def new_task(self, **kwargs):
        self.update_missing(
            kwargs, type="testing", name="test task models"
        )
        return self.create_temp("tasks", **kwargs)

    def new_model(self, name: str, **kwargs):
        return self.create_temp(
            "models", uri="file://test", name=name, labels={}, **kwargs
        )
