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
from apiserver.database.model.pipeline import Projectextendpipeline,PipelineNode
from apiserver import database
from .pipelinejinjagenerator import create_pipeline
from .pipelinecompile import PipeLineWithConnectionCompile
import subprocess
from apiserver.bll.project.project_bll import ProjectBLL
import os
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
        pipeline_setting: dict=None
    )-> str :
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
            p_id=ProjectBLL.find_or_create(user=user,company=company,system_tags=["hidden"],project_name=f'{project_obj.name}/.pipelines',
                                    description=description)
        
        pipeline = Projectextendpipeline(
            id=database.utils.id(),
            user=user,
            company=company,
            name=f'{project_obj.name}/.pipelines/{name}',
            basename=name,
            description=description,
            tags=tags,
            system_tags=["hidden","pipeline"],
            default_output_destination=default_output_destination,
            created=now,
            last_update=now,
            parent= p_id, 
            path ={p_id,project},
            parameters= parameters,
            flow_display=flow_display,
            pipeline_setting=pipeline_setting
        )
        pipeline.save()
        return pipeline.id
    
    @classmethod
    def verify_node_name(cls,node_name,pipeline_id):
        pipeline = Projectextendpipeline.objects(id=pipeline_id).first()
        if not pipeline:
            raise errors.bad_request.InvalidPipelineId(id= pipeline_id)
        if pipeline.node_exists(node_name):
            raise errors.bad_request.NodeExistence(
                f"Node named {node_name} already exists in the pipeline dag"
            )
        if pipeline.basename == node_name:
            raise errors.bad_request.ValidationError(
                f"Node named {node_name} is a reserved keyword for pipeline name, use a different name"
            )
        
    @classmethod
    def create_step(
        cls,
        name: str,
        description: str = "",
        parameters: dict= None,
        experiment:str = "",
        pipeline:str = "",
        code: str="",
        experiment_details : dict=None
    ) -> str:
        """
        Create a new step.
        Returns pipeline ID
        """
        now = datetime.utcnow()
        pipeline_node = PipelineNode(
            id=database.utils.id(),
            name=name,
            experiment = experiment,
            description=description,
            created=now,
            last_update=now,
            parameters= parameters,
            code =code,
            experiment_details= experiment_details
        )
        nodes_flow_display={
		"position": {
			"x": 0,
			"y": 0,
		},
		"sourcePosition": "right",
		"targetPosition": "left",
		"type": "normal"
	    }
        nodes_flow_display["id"]=pipeline_node.id
        nodes_flow_display['data']= pipeline_node
        pipeline= Projectextendpipeline.objects(id= pipeline).first()
        pipeline.nodes.append(pipeline_node)
        if pipeline.flow_display.get("nodes"):
            pipeline.flow_display["nodes"].append(nodes_flow_display)
        else:
            pipeline.flow_display["nodes"] = [nodes_flow_display]
        pipeline.save()
        return pipeline_node.id

    @classmethod
    def update_flow_display(cls,pipeline,node_obj):
        flow_display = pipeline.flow_display
        print(dir(node_obj))
        for node in flow_display["nodes"]:
            if node['id']==node_obj.id:
                node['data']=node_obj.to_mongo()
        pipeline.flow_display= flow_display
        pipeline.save()

    @classmethod
    def update_node(cls,pipeline:str,node:str,
                    parameters:list,code:str,description:str)->dict:
        pipeline_node = Projectextendpipeline.objects.get(id=pipeline).nodes.filter(id=node)
        if not pipeline_node:
            raise errors.bad_request.InvalidNodeId(id=node)
        
        pipeline_node[0].parameters = parameters
        pipeline_node[0].code = code
        pipeline_node[0].description=description
        pipeline_node.save()
        pipeline = Projectextendpipeline.objects(id=pipeline).first()
        cls.update_flow_display(pipeline,pipeline_node[0])
        pipeline_step_data = pipeline_node[0].to_mongo()
        pipeline_step_data['id']= pipeline_step_data['_id']
        return pipeline_step_data
    
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
        create_pipeline(pipeline_compile.compiled_json,pipeline_id)
        return True
    
    @classmethod
    def run(
        cls,
        pipeline_id: str

    ) -> bool:
        """
        Run pipeline
        """
        try:
            subprocess.Popen(['python',f"apiserver/Pipelines/{pipeline_id}.py"])
        except Exception:
            return False
        return True
    
    @classmethod
    def delete_step(cls, pipeline:str,node:str):
         
        pipeline = Projectextendpipeline.objects.get(id=pipeline).nodes.filter(id=node)
        if not pipeline:
            raise errors.bad_request.ValidationError("Nodes doesn't exists in pipeline")
        Projectextendpipeline.objects(id = pipeline).update_one(
        pull__nodes__id=node
                )
       

    @classmethod 
    def get_pipeline_code(cls,pipeline_id):

        if os.path.isfile(f"apiserver/Pipelines/{pipeline_id}.py"):
            with open(f"apiserver/Pipelines/{pipeline_id}.py" , 'r') as file :
                pipeline_code = file.read()
            return pipeline_code
        return ""