from jinja2 import Environment,FileSystemLoader
import json
import os
import sys


file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('clearml_pipeline.jinja2')

def create_pipeline(json_data,pipeline_id):

    with open(f"apiserver/Pipelines/{pipeline_id}.py","w") as f:
        f.write(template.render(json_data))

