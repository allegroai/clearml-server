
from datetime import date
import networkx as nx
from apiserver.database.model.pipeline import Pipeline,PipelineStep
from mongoengine import Q
import json
class PipeLineWithConnectionCompile(object):
    """
    Class having uitlity functions used make a pipeline
    flow,compile and trigger pipeline.
    """
    def __init__(self, nodes, edages, pipeline_id):
        self.nodes = nodes
        self.edages = edages
        self.compiled_json = {}
        self.pipeline_id = pipeline_id
        self.graph = nx.DiGraph()
        self.topological_order = self.add_nodes_and_edages()
        self.add_pipeline_detail()

    def add_nodes_and_edages(self):
        """
        This function adds nodes and edges in graph.
        Returns:
            list : returns nodes in a topological order.
        """
        for i in self.nodes:
            self.graph.add_node(i["nodeName"])
        for i in self.edages:
            self.graph.add_edge(i["startNodeName"], i["endNodeName"])
        return list(nx.topological_sort(self.graph))

    def get_dependency(self):
        """
        This function is used to get the dependencies of the nodes.
        Returns:
            dict : returns a dict.
        """
        temp = {}
        x = [n for n, d in self.graph.in_degree() if d == 0]
        temp["rootName"] = x
        y = [x for x in self.graph.nodes() if self.graph.out_degree(x) == 0]
        temp["leafNode"] = y
        topo=list(nx.topological_sort(self.graph))
        for i in topo:
            adj_node = list(self.graph.adj[i])

            if len(adj_node) > 0:
                temp[i] = adj_node
        return temp

    def add_pipeline_detail(self):
        """
        This function is used to add the pipeLineDetail.
        """
        query = Q(id=self.pipeline_id)
        pipeline = Pipeline.objects(query).first()
        self.compiled_json["pipeline_name"]=pipeline.name
        self.compiled_json['project_name'] = pipeline.project
        self.compiled_json['queue'] = "default"
        self.compiled_json["parameters"] = pipeline.parameters
        self.add_tasks()

    def set_parents(self):
        temp={}
        for key in self.nodes:
            temp[key["nodeName"]]=[]
        for i in self.edages:
            query = Q(id=i['startNodeName'])
            pipeline_step = PipelineStep.objects(query).first()
            temp[i['endNodeName']].append(pipeline_step.name)
        
        return temp
    def get_parents(self,parent_dic,node_id):
        return  parent_dic.get(node_id)

    def add_tasks(self):
        """
        This function is used to add the tasks.
        """
        tasks = []
        parent_dependcy= self.set_parents()
        for i in self.topological_order:
            temp = {}
            query = Q(id=i)
            pipeline_step = PipelineStep.objects(query).first()
            temp['name'] =pipeline_step.name
            temp['base_task_id']= pipeline_step.experiment
            temp['parents'] = self.get_parents(parent_dependcy,i)
            if len(pipeline_step.parameters)>0:
                temp['parameters'] = pipeline_step.parameters[0]
            else:
                temp['parameters']= {}
            tasks.append(temp)

        self.compiled_json["tasks"] = tasks
        self.add_task_order()

    def add_task_order(self):
        """
        This function is used to add the order of the tasks.
        """
        res = self.get_dependency()
        print(res)
        self.compiled_json["PipeLineFlow"] = res
    
    def save_json(self):
        """
        This function is used to save the configuration file.
        Returns:
            boolean : returns True if the configuration file save succesfully.
        """
        # print(self.compiled_json)
        with open(f'apiserver/Pipelines/{self.compiled_json["pipeline_name"]}.json', 'w', encoding='utf-8') as f:
            json.dump(self.compiled_json, f, ensure_ascii=False, indent=4)
        print("save Done")
        return True