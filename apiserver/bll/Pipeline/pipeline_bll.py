from typing import (
    Sequence,
    Optional,
    Type,
    Tuple,
    Dict,
    Set,
    TypeVar,
    Callable,
    Mapping,
    Any,
    Union,
)
import asyncio
from apiserver.apierrors import errors
from apiserver.database.model.project import Project
from mongoengine import Q
from datetime import datetime, timedelta
from apiserver.database.model.pipeline import Pipeline,PipelineStep
from apiserver import database
from .pipelinejinjagenerator import create_pipeline
from .pipelinecompile import PipeLineWithConnectionCompile
import subprocess
from apiserver.bll.project.project_bll import ProjectBLL
class PipelineBLL:

    @classmethod
    def create(
        cls,
        user: str,
        company: str,
        name: str,
        description: str = "",
        project : str= "",
        tags: Sequence[str] = None,
        system_tags: Sequence[str] = None,
        default_output_destination: str = None,
        parameters: dict= None,
        flow_display : dict=None,
        parent_creation_params: dict = None,
    ) -> str:
        """
        Create a new pipeline.
        Returns pipeline ID
        """
        now = datetime.utcnow()

        if not project:
            raise errors.bad_request.ValidationError("project id or name required")

        if project:
            query = Q(id=project)
            project_obj = Project.objects(query).first()
            if not project:
                raise errors.bad_request.InvalidProjectId(id=project)
            ProjectBLL.create(user=user,company=company,system_tags=["pipeline"],name=f'{project_obj.name}/.pipelines/{name}',
                                    description=description)

        pipeline = Pipeline(
            id=database.utils.id(),
            user=user,
            company=company,
            name=name,
            basename=name.split("/")[-1],
            description=description,
            tags=tags,
            system_tags=system_tags,
            default_output_destination=default_output_destination,
            created=now,
            last_update=now,
            parameters= parameters,
            project=project,
            flow_display=flow_display
        )
        pipeline.save()
        return pipeline.id
    
    @classmethod
    def create_step(
        cls,
        user: str,
        company: str,
        name: str,
        description: str = "",
        tags: Sequence[str] = None,
        system_tags: Sequence[str] = None,
        default_output_destination: str = None,
        parameters: dict= None,
        experiment:str = "",
        pipeline_id:str = "",
    ) -> str:
        """
        Create a new step.
        Returns pipeline ID
        """
        now = datetime.utcnow()
        pipeline_step = PipelineStep(
            id=database.utils.id(),
            user=user,
            company=company,
            name=name,
            experiment = experiment,
            basename=name.split("/")[-1],
            description=description,
            tags=tags,
            system_tags=system_tags,
            default_output_destination=default_output_destination,
            created=now,
            last_update=now,
            parameters= parameters,
            pipeline_id= pipeline_id,
        )
        pipeline_step.save()

        return pipeline_step.id
    
    @classmethod
    def compile(
        cls,
        steps : list,
        connections: list,
        pipeline_id: str

    ) -> bool:
        """
        Compile pipeline
        """
        pipeline_compile= PipeLineWithConnectionCompile(steps,connections,pipeline_id)
        create_pipeline(pipeline_compile.compiled_json)

        return True