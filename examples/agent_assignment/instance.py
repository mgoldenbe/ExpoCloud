"""
The classes related to the agent assignment problem and the function for generating the problem instances.
"""

import random
from typing import List
from src.abstract_task import AbstractProblemInstance

class AATask:
    """
    A task in the agent assignment problem.
    """    
    def __init__(self, id: int, agent_costs: List[float]):
        """
        The constructor

        :param id: Task id.
        :type id: int
        :param agent_costs: For each agent, the time needed to complete the task.
        :type agent_costs: List[float]
        """        
        self.id = id
        self.agent_costs = agent_costs
    
    def id(self) -> int:
        """
        Return the task id.

        :return: The task id.
        :rtype: int
        """        
        return self._id
    
    def __str__(self) -> str:
        """
        Return the string representation of the task.

        :return: The string representation of the task.
        :rtype: str
        """        
        return f"Task {self.id}: {self.agent_costs}"

class ProblemInstance(AbstractProblemInstance):
    """
    An instance of the agent assignment problem.
    """    
    def __init__(self, id: int, tasks: List[AATask]):
        """
        The constructor

        :param id: Instance id.
        :type id: int
        :param tasks: The list of tasks to which agents need to be optimally assigned.
        :type tasks: List[AATask]
        """        
        assert(len(tasks) > 0)
        self.tasks = tasks
        self.n_tasks = len(self.tasks)
        self.n_agents = len(self.tasks[0].agent_costs)
        super(ProblemInstance, self).__init__(id)
    
    def parameter_titles(self) -> List[str]:
        """
        Return the tuple of names of the parameters characterizing the instance.

        :return: The tuple of names of the parameters characterizing the instance.
        :rtype: List[str]
        """        
        return ("id", "|T|", "|A|")

    # Output id, number of tasks, number of suppliers per task, 
    # number of times per supplier, m_super
    def parameters(self) -> tuple:
        """
        Return the tuple of parameters characterizing the instance.

        :return: The tuple of parameters characterizing the instance.
        :rtype: tuple
        """
        return (self.id, self.n_tasks, self.n_agents)
        
    def __str__(self) -> str:
        """
        Return the string representation of the instance.

        :return: The string representation of the instance.
        :rtype: str
        """      
        result = f"Instance {self.id}\n"
        result += "\n" + '\n'.join([str(t) for t in self.tasks])  + "\n"
        result += '\n\n------------------------------------------------------\n'
        return result

# returns list of tuples (id, tasks)
# instance with same id is always same
def generate_instances(
    n_tasks: int, n_agents: int, first_id: int, last_id: int) \
    -> List[ProblemInstance]:    
    """
    Generate instances of the agent assignment problem. The instance with the same id is guaranteed to be the same between the runs.

    :param n_tasks: The number of tasks to which agents will need to be optimally assigned.
    :type n_tasks: int
    :param n_agents: The number of agents.
    :type n_agents: int
    :param first_id: The id of the first instance.
    :type first_id: int
    :param last_id: The id of the first instance.
    :type last_id: int
    :return: The generated instances.
    :rtype: List[ProblemInstance]
    """    
    assert(n_agents >= n_tasks)
    random.seed(47)
    instances = []
    for i in range(last_id + 1):
        instance = ProblemInstance(i, [AATask(id, [random.randint(1,100) for _ in range(n_agents)]) for id in range(n_tasks)])
        if i >= first_id: 
            instances.append(instance)
    return instances