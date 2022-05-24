from src.abstract_task import AbstractTask
from examples.samd.bnb import Algorithm

class Task(AbstractTask):
    def __init__(self, options, instance, m = -1, timeout = 60):
        self.options = options
        self.instance = instance
        self.m = m
        self.timeout = timeout
        self.algorithm = Algorithm(self.options, self.instance, self.m)
        super(Task, self).__init__()

    def parameter_titles(self):
        return self.instance.parameter_titles() + ("Options", "m")
        
    def parameters(self):
        return self.instance.parameters() + (self.options, self.m)
    
    def result_titles(self):
        return self.algorithm.result_titles()

    def hardness_parameters(self):
        tasks = self.instance.tasks
        suppliers = tasks[0].suppliers()
        return (self.m, len(tasks), len(suppliers))

    def run(self):
        return self.algorithm.search()