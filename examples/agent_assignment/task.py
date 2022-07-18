from src.abstract_task import AbstractTask
from src.util import filter_out, set2str
from examples.agent_assignment.bnb import Algorithm

class Task(AbstractTask):
    def __init__(self, options, instance, timeout = 60):
        self.options = options
        self.instance = instance
        self.timeout = timeout
        self.algorithm = Algorithm(self.options, self.instance)
        super(Task, self).__init__()

    def group_parameter_titles(self):
        return filter_out(self.parameter_titles(), ('id',))

    def parameter_titles(self):
        return self.instance.parameter_titles() + ("Options",)
        
    def parameters(self):
        return self.instance.parameters() + (set2str(self.options),)
    
    def result_titles(self):
        return self.algorithm.result_titles()

    def hardness_parameters(self):
        return (self.instance.n_tasks, self.instance.n_agents)

    def run(self):
        return self.algorithm.search()