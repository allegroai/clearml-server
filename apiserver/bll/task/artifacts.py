from hashlib import md5
from operator import itemgetter
from typing import Sequence

from apiserver.apimodels.tasks import Artifact as ApiArtifact, ArtifactId
from apiserver.bll.task.utils import get_task_for_update, update_task
from apiserver.database.model.task.task import DEFAULT_ARTIFACT_MODE, Artifact
from apiserver.timing_context import TimingContext
from apiserver.utilities.dicts import nested_get, nested_set
from apiserver.utilities.parameter_key_escaper import mongoengine_safe


def get_artifact_id(artifact: dict):
    """
    Calculate id from 'key' and 'mode' fields
    Return hash on on the id so that it will not contain mongo illegal characters
    """
    key_hash: str = md5(artifact["key"].encode()).hexdigest()
    mode: str = artifact.get("mode", DEFAULT_ARTIFACT_MODE)
    return f"{key_hash}_{mode}"


def artifacts_prepare_for_save(fields: dict):
    artifacts_field = ("execution", "artifacts")
    artifacts = nested_get(fields, artifacts_field)
    if artifacts is None:
        return

    nested_set(
        fields, artifacts_field, value={get_artifact_id(a): a for a in artifacts}
    )


def artifacts_unprepare_from_saved(fields):
    artifacts_field = ("execution", "artifacts")
    artifacts = nested_get(fields, artifacts_field)
    if artifacts is None:
        return

    nested_set(
        fields,
        artifacts_field,
        value=sorted(artifacts.values(), key=itemgetter("key", "mode")),
    )


class Artifacts:
    @classmethod
    def add_or_update_artifacts(
        cls,
        company_id: str,
        task_id: str,
        artifacts: Sequence[ApiArtifact],
        force: bool,
    ) -> int:
        with TimingContext("mongo", "update_artifacts"):
            task = get_task_for_update(
                company_id=company_id,
                task_id=task_id,
                force=force,
            )

            artifacts = {
                get_artifact_id(a): Artifact(**a)
                for a in (api_artifact.to_struct() for api_artifact in artifacts)
            }

            update_cmds = {
                f"set__execution__artifacts__{mongoengine_safe(name)}": value
                for name, value in artifacts.items()
            }
            return update_task(task, update_cmds=update_cmds)

    @classmethod
    def delete_artifacts(
        cls,
        company_id: str,
        task_id: str,
        artifact_ids: Sequence[ArtifactId],
        force: bool,
    ) -> int:
        with TimingContext("mongo", "delete_artifacts"):
            task = get_task_for_update(
                company_id=company_id,
                task_id=task_id,
                force=force,
            )

            artifact_ids = [
                get_artifact_id(a)
                for a in (artifact_id.to_struct() for artifact_id in artifact_ids)
            ]
            delete_cmds = {
                f"unset__execution__artifacts__{id_}": 1 for id_ in set(artifact_ids)
            }

            return update_task(task, update_cmds=delete_cmds)
