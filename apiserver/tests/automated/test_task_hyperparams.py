from operator import itemgetter
from typing import Sequence, List, Tuple

from boltons import iterutils

from apiserver.apierrors.errors.bad_request import InvalidTaskStatus
from apiserver.tests.api_client import APIClient
from apiserver.tests.automated import TestService


class TestTasksHyperparams(TestService):
    def setUp(self, **kwargs):
        super().setUp(version="2.9")

    def new_task(self, **kwargs) -> Tuple[str, str]:
        if "project" not in kwargs:
            kwargs["project"] = self.create_temp(
                "projects",
                name="Test hyperparams",
                description="test",
                delete_params=dict(force=True),
            )
        self.update_missing(
            kwargs,
            type="testing",
            name="test hyperparams",
            delete_params=dict(force=True),
        )
        return self.create_temp("tasks", **kwargs), kwargs["project"]

    def test_hyperparams(self):
        legacy_params = {"legacy$1": "val1", "legacy2/name": "val2"}
        new_params = [
            dict(section="1/1", name="param1/1", type="type1", value="10"),
            dict(section="1/1", name="param2", type="type1", value="20"),
            dict(section="2", name="param2", type="type2", value="xxx"),
        ]
        new_params_dict = self._param_dict_from_list(new_params)
        task, project = self.new_task(
            execution={"parameters": legacy_params}, hyperparams=new_params_dict,
        )
        # both params and hyper params are set correctly
        old_params = self._new_params_from_legacy(legacy_params)
        params_dict = new_params_dict.copy()
        params_dict["Args"] = {p["name"]: p for p in old_params}
        res = self.api.tasks.get_by_id(task=task).task
        self.assertEqual(params_dict, res.hyperparams)

        # returned as one list with params in the _legacy section
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(new_params + old_params, res.hyperparams)

        # replace section
        replace_params = [
            dict(section="1/1", name="param1", type="type1", value="40"),
            dict(section="2", name="param5", type="type1", value="11"),
        ]
        self.api.tasks.edit_hyper_params(
            task=task, hyperparams=replace_params, replace_hyperparams="section"
        )
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(replace_params + old_params, res.hyperparams)

        # replace all
        replace_params = [
            dict(section="1/1", name="param1/1", type="type1", value="30"),
            dict(section="Args", name="legacy$1", value="123", type="legacy"),
        ]
        self.api.tasks.edit_hyper_params(
            task=task, hyperparams=replace_params, replace_hyperparams="all"
        )
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(replace_params, res.hyperparams)

        # add and update
        self.api.tasks.edit_hyper_params(task=task, hyperparams=new_params + old_params)
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(new_params + old_params, res.hyperparams)

        # delete
        new_to_delete = self._get_param_keys(new_params[1:])
        old_to_delete = self._get_param_keys(old_params[:1])
        self.api.tasks.delete_hyper_params(
            task=task, hyperparams=new_to_delete + old_to_delete
        )
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(new_params[:1] + old_params[1:], res.hyperparams)

        # delete section
        self.api.tasks.delete_hyper_params(
            task=task, hyperparams=[{"section": "1/1"}, {"section": "2"}]
        )
        res = self.api.tasks.get_hyper_params(tasks=[task]).params[0]
        self.assertEqual(old_params[1:], res.hyperparams)

        # project hyperparams
        res = self.api.projects.get_hyper_parameters(project=project)
        self.assertEqual(
            [
                {k: v for k, v in p.items() if k in ("section", "name")}
                for p in old_params[1:]
            ],
            res.parameters,
        )

        # clone task
        new_task = self.api.tasks.clone(
            task=task, new_task_hyperparams=new_params_dict
        ).id
        try:
            res = self.api.tasks.get_hyper_params(tasks=[new_task]).params[0]
            self.assertEqual(new_params, res.hyperparams)
        finally:
            self.api.tasks.delete(task=new_task, force=True)

        # editing of started task
        self.api.tasks.started(task=task)
        with self.api.raises(InvalidTaskStatus):
            self.api.tasks.edit_hyper_params(
                task=task, hyperparams=[dict(section="test", name="x", value="123")]
            )
        with self.api.raises(InvalidTaskStatus):
            self.api.tasks.delete_hyper_params(
                task=task, hyperparams=[dict(section="test")]
            )
        self.api.tasks.edit_hyper_params(
            task=task,
            hyperparams=[dict(section="test", name="x", value="123")],
            force=True,
        )
        self.api.tasks.delete_hyper_params(
            task=task, hyperparams=[dict(section="test")], force=True
        )

        # properties section can be edited/deleted in any task state without the flag
        self.api.tasks.edit_hyper_params(
            task=task, hyperparams=[dict(section="properties", name="x", value="123")]
        )
        self.api.tasks.delete_hyper_params(
            task=task, hyperparams=[dict(section="properties")]
        )

    @staticmethod
    def _get_param_keys(params: Sequence[dict]) -> List[dict]:
        return [{k: p[k] for k in ("name", "section")} for p in params]

    @staticmethod
    def _new_params_from_legacy(legacy: dict) -> List[dict]:
        return [
            dict(section="Args", name=k, value=str(v), type="legacy")
            if not k.startswith("TF_DEFINE/")
            else dict(
                section="TF_DEFINE",
                name=k[len("TF_DEFINE/") :],
                value=str(v),
                type="legacy",
            )
            for k, v in legacy.items()
        ]

    @staticmethod
    def _param_dict_from_list(params: Sequence[dict]) -> dict:
        return {
            k: {v["name"]: v for v in values}
            for k, values in iterutils.bucketize(
                params, key=itemgetter("section")
            ).items()
        }

    @staticmethod
    def _config_dict_from_list(config: Sequence[dict]) -> dict:
        return {c["name"]: c for c in config}

    def test_configuration(self):
        legacy_config = {"design": "hello"}
        new_config = [
            dict(name="param$1", type="type1", value="10"),
            dict(name="param/2", type="type1", value="20"),
            dict(name="param_empty", type="type1", value=""),
        ]
        new_config_dict = self._config_dict_from_list(new_config)
        task, _ = self.new_task(
            execution={"model_desc": legacy_config}, configuration=new_config_dict
        )

        # both params and hyper params are set correctly
        old_config = self._new_config_from_legacy(legacy_config)
        config_dict = new_config_dict.copy()
        config_dict["design"] = old_config[0]
        res = self.api.tasks.get_by_id(task=task).task
        self.assertEqual(config_dict, res.configuration)

        # returned as one list
        res = self.api.tasks.get_configurations(tasks=[task]).configurations[0]
        self.assertEqual(old_config + new_config, res.configuration)

        # names
        res = self.api.tasks.get_configuration_names(tasks=[task]).configurations[0]
        self.assertEqual(task, res.task)
        self.assertEqual(
            ["design", *[c["name"] for c in new_config if c["value"]]], res.names
        )
        res = self.api.tasks.get_configuration_names(
            tasks=[task], skip_empty=False
        ).configurations[0]
        self.assertEqual(task, res.task)
        self.assertEqual(["design", *[c["name"] for c in new_config]], res.names)

        # returned as one list with names filtering
        res = self.api.tasks.get_configurations(
            tasks=[task], names=[new_config[1]["name"]]
        ).configurations[0]
        self.assertEqual([new_config[1]], res.configuration)

        # replace all
        replace_configs = [
            dict(name="design", value="123", type="legacy"),
            dict(name="param/2", type="type1", value="30"),
        ]
        self.api.tasks.edit_configuration(
            task=task, configuration=replace_configs, replace_configuration=True
        )
        res = self.api.tasks.get_configurations(tasks=[task]).configurations[0]
        self.assertEqual(replace_configs, res.configuration)

        # add and update
        self.api.tasks.edit_configuration(
            task=task, configuration=new_config + old_config
        )
        res = self.api.tasks.get_configurations(tasks=[task]).configurations[0]
        self.assertEqual(old_config + new_config, res.configuration)

        # delete
        new_to_delete = self._get_config_keys(new_config[1:])
        self.api.tasks.delete_configuration(task=task, configuration=new_to_delete)
        res = self.api.tasks.get_configurations(tasks=[task]).configurations[0]
        self.assertEqual(old_config + new_config[:1], res.configuration)

        # clone task
        new_task = self.api.tasks.clone(
            task=task, new_task_configuration=new_config_dict
        ).id
        try:
            res = self.api.tasks.get_configurations(tasks=[new_task]).configurations[0]
            self.assertEqual(new_config, res.configuration)
        finally:
            self.api.tasks.delete(task=new_task, force=True)

        # edit/delete of running task
        self.api.tasks.started(task=task)
        with self.api.raises(InvalidTaskStatus):
            self.api.tasks.edit_configuration(task=task, configuration=new_config)
        with self.api.raises(InvalidTaskStatus):
            self.api.tasks.delete_configuration(task=task, configuration=new_to_delete)
        self.api.tasks.edit_configuration(
            task=task, configuration=new_config, force=True
        )
        self.api.tasks.delete_configuration(
            task=task, configuration=new_to_delete, force=True
        )

    @staticmethod
    def _get_config_keys(config: Sequence[dict]) -> List[dict]:
        return [c["name"] for c in config]

    @staticmethod
    def _new_config_from_legacy(legacy: dict) -> List[dict]:
        return [dict(name=k, value=str(v), type="legacy") for k, v in legacy.items()]

    def test_hyperparams_projection(self):
        legacy_param = {"legacy.1": "val1"}
        new_params1 = [
            dict(section="sec.tion1", name="param1", type="type1", value="10")
        ]
        new_params_dict1 = self._param_dict_from_list(new_params1)
        task1, project = self.new_task(
            execution={"parameters": legacy_param}, hyperparams=new_params_dict1,
        )

        new_params2 = [
            dict(section="sec.tion1", name="param1", type="type1", value="20")
        ]
        new_params_dict2 = self._param_dict_from_list(new_params2)
        task2, _ = self.new_task(hyperparams=new_params_dict2, project=project)

        old_params = self._new_params_from_legacy(legacy_param)
        params_dict = new_params_dict1.copy()
        params_dict["Args"] = {p["name"]: p for p in old_params}
        res = self.api.tasks.get_all_ex(id=[task1], only_fields=["hyperparams"]).tasks[
            0
        ]
        self.assertEqual(params_dict, res.hyperparams)

        res = self.api.tasks.get_all_ex(
            project=[project],
            only_fields=["hyperparams.sec%2Etion1"],
            order_by=["-hyperparams.sec%2Etion1"],
        ).tasks[0]
        self.assertEqual(new_params_dict2, res.hyperparams)

    def test_numeric_ordering(self):
        params = [
            dict(section="section1", name="param1", type="type1", value="1"),
            dict(section="section1", name="param1", type="type1", value="2"),
            dict(section="section1", name="param1", type="type1", value="11"),
        ]
        tasks = [
            self.new_task(hyperparams=self._param_dict_from_list([p]), project=None)[0]
            for p in params
        ]

        res = self.api.tasks.get_all_ex(id=tasks, order_by=["hyperparams.section1.param1"]).tasks
        self.assertEqual([t.id for t in res], tasks)

        res = self.api.tasks.get_all_ex(id=tasks, order_by=["-hyperparams.section1.param1"]).tasks
        self.assertEqual([t.id for t in res], list(reversed(tasks)))

    def test_old_api(self):
        legacy_params = {"legacy.1": "val1", "TF_DEFINE/param2": "val2"}
        legacy_config = {"design": "hello"}
        task_id, _ = self.new_task(
            execution={"parameters": legacy_params, "model_desc": legacy_config}
        )
        config = self._config_dict_from_list(
            self._new_config_from_legacy(legacy_config)
        )
        params = self._param_dict_from_list(self._new_params_from_legacy(legacy_params))

        old_api = APIClient(base_url="http://localhost:8008/v2.8")
        task = old_api.tasks.get_all_ex(id=[task_id]).tasks[0]
        self.assertEqual(legacy_params, task.execution.parameters)
        self.assertEqual(legacy_config, task.execution.model_desc)
        self.assertEqual(params, task.hyperparams)
        self.assertEqual(config, task.configuration)

        modified_params = {"legacy.2": "val2"}
        modified_config = {"design": "by"}
        old_api.tasks.edit(
            task=task_id,
            execution=dict(parameters=modified_params, model_desc=modified_config),
        )
        task = old_api.tasks.get_all_ex(id=[task_id]).tasks[0]
        self.assertEqual(modified_params, task.execution.parameters)
        self.assertEqual(modified_config, task.execution.model_desc)
