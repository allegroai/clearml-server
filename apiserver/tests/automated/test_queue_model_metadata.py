from functools import partial
from typing import Sequence

from apiserver.tests.api_client import APIClient
from apiserver.tests.automated import TestService


class TestQueueAndModelMetadata(TestService):
    def setUp(self, version="2.13"):
        super().setUp(version=version)

    meta1 = [{"key": "test_key", "type": "str", "value": "test_value"}]

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
        self.api.models.edit(model=model_id, metadata=[self.meta1[0]])
        self._assertMeta(service=service, entity=entity, _id=model_id, meta=self.meta1)

    def _test_meta_operations(
        self, service: APIClient.Service, entity: str, _id: str,
    ):
        assert_meta = partial(self._assertMeta, service=service, entity=entity)
        assert_meta(_id=_id, meta=self.meta1)

        meta2 = [
            {"key": "test1", "type": "str", "value": "data1"},
            {"key": "test2", "type": "str", "value": "data2"},
            {"key": "test3", "type": "str", "value": "data3"},
        ]
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
        assert_meta(_id=_id, meta=[meta2[0], *updates])

        res = service.delete_metadata(
            **{entity: _id, "keys": [f"test{idx}" for idx in range(2, 6)]}
        )
        self.assertEqual(res.updated, 1)
        assert_meta(_id=_id, meta=meta2[:1])

    def _assertMeta(
        self, service: APIClient.Service, entity: str, _id: str, meta: Sequence[dict]
    ):
        res = service.get_all_ex(id=[_id])[f"{entity}s"][0]
        self.assertEqual(res.metadata, meta)

    def _temp_queue(self, name, **kwargs):
        return self.create_temp("queues", name=name, **kwargs)

    def _temp_model(self, name: str, **kwargs):
        return self.create_temp(
            "models", uri="file://test", name=name, labels={}, **kwargs
        )
