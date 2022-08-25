"""
The Task class for the agent assignment example experiment.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from typing import Tuple
from examples.agent_assignment.bnb import Algorithm, Option
from src.abstract_task import AbstractTask
from src.util import filter_out, set2str

class Task(AbstractTask):
    def __init__(self, algorithm: Algorithm, timeout = 60):
        """
        The constructor.

        :param algorithm: The algorithm used to solve the problem instance.
        :type algorithm: Algorithm
        :param timeout: The deadline for the task in seconds.
        :type timeout: float
        """
        super(Task, self).__init__(algorithm, timeout)

    def group_parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that determine groups for counting the number of non-hard instances.

        :return: The tuple of names of parameters that determine groups for counting the number of non-hard instances.
        :rtype: Tuple[str]
        """
        return filter_out(self.parameter_titles(), ('id',))

    def parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that characterize the task.

        :return: The tuple of names of parameters that characterize the task.
        :rtype: Tuple[str]
        """
        return self.instance.parameter_titles() + ("Options",)
        
    def parameters(self) -> tuple:
        """
        Return the tuple of parameters that characterize the task.

        :return: The tuple of parameters that characterize the task.
        :rtype: tuple
        """
        return self.instance.parameters() + (set2str(self.options),)

    def hardness_parameters(self) -> tuple:
        """
        Return the tuple of parameters determining the hardness of the task. This is to be used to initialize the Hardness object.

        :return: The tuple of parameters determining the hardness of the task. 
        :rtype: tuple
        """
        def options2hardness(options):
            if Option.HEURISTIC in options: return 0
            if Option.NO_CUTOFFS in options: return 2
            return 1
                   
        return (
            options2hardness(self.options), 
            self.instance.n_tasks, 
            self.instance.n_agents)

    def result_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of output values for the solved task.

        :return: The tuple of names of output values for the solved task.
        :rtype: Tuple[str]
        """
        return self.algorithm.result_titles()

    def run(self) -> tuple:
        """
        Return the tuple of output values for the solved task.

        :return: The tuple of output values for the solved task.
        :rtype: tuple
        """
        return self.algorithm.search()