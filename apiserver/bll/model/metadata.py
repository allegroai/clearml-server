from typing import Sequence, Union, Mapping

from mongoengine import Document

from apiserver.apierrors import errors
from apiserver.apimodels.metadata import MetadataItem
from apiserver.database.model.base import GetMixin
from apiserver.utilities.parameter_key_escaper import (
    ParameterKeyEscaper,
    mongoengine_safe,
)
from apiserver.config_repo import config

log = config.logger(__file__)


class Metadata:
    @staticmethod
    def metadata_from_api(
        api_data: Union[Mapping[str, MetadataItem], Sequence[MetadataItem]]
    ) -> dict:
        if not api_data:
            return {}

        if isinstance(api_data, dict):
            return {
                ParameterKeyEscaper.escape(k): v.to_struct()
                for k, v in api_data.items()
            }

        return {
            ParameterKeyEscaper.escape(item.key): item.to_struct() for item in api_data
        }

    @classmethod
    def edit_metadata(
        cls,
        obj: Document,
        items: Sequence[MetadataItem],
        replace_metadata: bool,
        **more_updates,
    ) -> int:
        update_cmds = dict()
        metadata = cls.metadata_from_api(items)
        if replace_metadata:
            update_cmds["set__metadata"] = metadata
        else:
            for key, value in metadata.items():
                update_cmds[f"set__metadata__{mongoengine_safe(key)}"] = value

        return obj.update(**update_cmds, **more_updates)

    @classmethod
    def delete_metadata(cls, obj: Document, keys: Sequence[str], **more_updates) -> int:
        return obj.update(
            **{
                f"unset__metadata__{ParameterKeyEscaper.escape(key)}": 1
                for key in set(keys)
            },
            **more_updates,
        )

    @staticmethod
    def _process_path(path: str):
        """
        Frontend does a partial escaping on the path so the all '.' in key names are escaped
        Need to unescape and apply a full mongo escaping
        """
        parts = path.split(".")
        if len(parts) < 2 or len(parts) > 3:
            raise errors.bad_request.ValidationError("invalid field", path=path)
        return ".".join(
            ParameterKeyEscaper.escape(ParameterKeyEscaper.unescape(p)) for p in parts
        )

    @classmethod
    def escape_paths(cls, paths: Sequence[str]) -> Sequence[str]:
        for prefix in (
            "metadata.",
            "-metadata.",
        ):
            paths = [
                cls._process_path(path) if path.startswith(prefix) else path
                for path in paths
            ]
        return paths

    @classmethod
    def escape_query_parameters(cls, call_data: dict) -> dict:
        if not call_data:
            return call_data

        keys = list(call_data)
        call_data = {
            safe_key: call_data[key]
            for key, safe_key in zip(keys, Metadata.escape_paths(keys))
        }

        projection = GetMixin.get_projection(call_data)
        if projection:
            GetMixin.set_projection(call_data, Metadata.escape_paths(projection))

        ordering = GetMixin.get_ordering(call_data)
        if ordering:
            GetMixin.set_ordering(call_data, Metadata.escape_paths(ordering))

        return call_data
