from functools import partial

from apiserver.tests.api_client import APIClient
from apiserver.tests.automated import TestService


class TestQueueAndModelMetadata(TestService):
    meta1 = {"test_key": {"key": "test_key", "type": "str", "value": "test_value"}}

    def test_queue_metas(self):
        queue_id = self._temp_queue("TestMetadata", metadata=self.meta1)
        self._test_meta_operations(
            service=self.api.queues, entity="queue", _id=queue_id
        )

    def test_models_metas(self):
        service = self.api.models
        entity = "model"
        model_id = self._temp_model("TestMetadata", metadata=self.meta1)
        self._test_meta_operations(
            service=self.api.models, entity="model", _id=model_id
        )

        model_id = self._temp_model("TestMetadata1")
        self.api.models.edit(model=model_id, metadata=self.meta1)
        self._assertMeta(service=service, entity=entity, _id=model_id, meta=self.meta1)

    def test_project_meta_query(self):
        self._temp_model("TestMetadata", metadata=self.meta1)
        project = self.temp_project(name="MetaParent")
        test_key = "test_key"
        test_key2 = "test_key2"
        test_value = "test_value"
        test_value2 = "test_value2"
        model_id = self._temp_model(
            "TestMetadata2",
            project=project,
            metadata={
                test_key: {"key": test_key, "type": "str", "value": test_value},
                test_key2: {"key": test_key2, "type": "str", "value": test_value2},
            },
        )
        res = self.api.projects.get_model_metadata_keys()
        self.assertTrue({test_key, test_key2}.issubset(set(res["keys"])))
        res = self.api.projects.get_model_metadata_keys(include_subprojects=False)
        self.assertTrue(test_key in res["keys"])
        self.assertFalse(test_key2 in res["keys"])

        model = self.api.models.get_all_ex(
            id=[model_id], only_fields=["metadata.test_key"]
        ).models[0]
        self.assertTrue(test_key in model.metadata)
        self.assertFalse(test_key2 in model.metadata)

        res = self.api.projects.get_model_metadata_values(key=test_key)
        self.assertEqual(res.total, 1)
        self.assertEqual(res["values"], [test_value])

    def _test_meta_operations(
        self, service: APIClient.Service, entity: str, _id: str,
    ):
        assert_meta = partial(self._assertMeta, service=service, entity=entity)
        assert_meta(_id=_id, meta=self.meta1)

        meta2 = {
            "test1": {"key": "test1", "type": "str", "value": "data1"},
            "test2": {"key": "test2", "type": "str", "value": "data2"},
            "test3": {"key": "test3", "type": "str", "value": "data3"},
        }
        service.update(**{entity: _id, "metadata": meta2})
        assert_meta(_id=_id, meta=meta2)

        updates = [
            {"key": "test2", "type": "int", "value": "10"},
            {"key": "test3", "type": "int", "value": "20"},
            {"key": "test4", "type": "array", "value": "xxx,yyy"},
            {"key": "test5", "type": "array", "value": "zzz"},
        ]
        res = service.add_or_update_metadata(**{entity: _id, "metadata": updates})
        self.assertEqual(res.updated, 1)
        assert_meta(_id=_id, meta={**meta2, **{u["key"]: u for u in updates}})

        res = service.delete_metadata(
            **{entity: _id, "keys": [f"test{idx}" for idx in range(2, 6)]}
        )
        self.assertEqual(res.updated, 1)
        # noinspection PyTypeChecker
        assert_meta(_id=_id, meta=dict(list(meta2.items())[:1]))

    def _assertMeta(
        self, service: APIClient.Service, entity: str, _id: str, meta: dict
    ):
        res = service.get_all_ex(id=[_id])[f"{entity}s"][0]
        self.assertEqual(res.metadata, meta)

    def _temp_queue(self, name, **kwargs):
        return self.create_temp("queues", name=name, **kwargs)

    def _temp_model(self, name: str, **kwargs):
        return self.create_temp(
            "models", uri="file://test", name=name, labels={}, **kwargs
        )

    def temp_project(self, **kwargs) -> str:
        self.update_missing(
            kwargs,
            name="Test models meta",
            description="test",
            delete_params=dict(force=True),
        )
        return self.create_temp("projects", **kwargs)
