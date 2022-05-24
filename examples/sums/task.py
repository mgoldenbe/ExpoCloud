
from src.abstract_task import AbstractTask

class Task(AbstractTask):
    def __init__(self, n, timeout):
        self.n = n
        self.timeout = timeout
        super(Task, self).__init__()
    
    def parameter_titles(self):
        return ("n",)

    def parameters(self):
        return (self.n,)

    def result_titles(self):
        return ("sum",)

    def hardness_parameters(self):
        return (self.n,)

    def run(self):
        sum = 0
        for i in range(1, self.n):
            sum += i
        return (sum,)