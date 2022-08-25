"""
Defines parent classes for all ProblemInstance, Algorithm and Task classes.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from __future__ import annotations
from typing import Tuple
from src import util

class AbstractProblemInstance:
    """
    This is a parent class for ProblemInstance classes, such as :any:`examples.agent_assignment.instance.ProblemInstance`.
    """    
    def __init__(self, id: int):
        """
        The constructor.

        :param id: Instance id.
        :type id: int
        """        
        self.id = id

class AbstractAlgorithm:
    """
    This is a parent class for Algorithm classes, such as :any:`examples.agent_assignment.bnb.Algorithm`.
    """    
    def __init__(self, options:set, instance: AbstractProblemInstance):
        """
        The constructor.

        :param options: The options controlling the behavior of the algorithm. 
        :type options: set
        :param instance: The problem instance
        :type instance: AbstractProblemInstance
        """        
        self.instance = instance
        self.options = options

class AbstractTask:
    """
    This is a parent class for Task classes for experiments. See :any:`examples.agent_assignment.task.Task` for an example.
    """

    class Hardness:
        """
        The default class to represent hardness of a task. It provides default comparators.
        """
        def __init__(self, params: tuple):
            """
            The constructor.

            :param params: The tuple of parameters determining the task's hardness.
            :type params: tuple
            """            
            self.params = params

        def __str__(self) -> str:
            """
            Return the string representation of hardness.

            :return: The string representation of hardness.
            :rtype: str
            """            
            return str(self.params)

        def __repr__(self) -> str:
            """
            Return the string representation of hardness.

            :return: The string representation of hardness.
            :rtype: str
            """
            return str(self)

        def __lt__(self, other: Hardness) -> bool:
            """
            Checks whether this hardness is strictly less than :paramref:`other`.

            :param other: The hardness to be compared to.
            :type other: Hardness
            :return: ``True`` if hardness is strictly less than :paramref:`other` and ``False`` otherwise.
            :rtype: bool
            """
            return util.tuple_lt(self.params, other.params)
        
        def __le__(self, other: Hardness) -> bool:
            """
            Checks whether this hardness is less than or equal to :paramref:`other`.

            :param other: The hardness to be compared to.
            :type other: Hardness
            :return: ``True`` if hardness is less than or equal to :paramref:`other` and ``False`` otherwise.
            :rtype: bool
            """
            return util.tuple_le(self.params, other.params)
        
    def __init__(self, algorithm:AbstractAlgorithm, timeout: float):
        """
        The constructor.

        :param algorithm: The algorithm used to solve the problem instance.
        :type algorithm: AbstractAlgorithm
        :param timeout: The deadline for the task in seconds.
        :type timeout: float
        """
        self.algorithm = algorithm      
        self.options = algorithm.options
        self.instance = algorithm.instance
        self.timeout = timeout
        self.hardness = self.Hardness(self.hardness_parameters())
        self.result = None

    def group_parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that determine groups for counting the number of non-hard instances.

        :return: The tuple of names of parameters that determine groups for counting the number of non-hard instances.
        :rtype: Tuple[str]
        """        
        return ()
    
    def group_parameters(self) -> tuple:
        """
        Return the tuple of parameters that determine groups for counting the number of non-hard instances.

        :return: The tuple of parameters that determine groups for counting the number of non-hard instances.
        :rtype: tuple
        """
        all_titles = self.parameter_titles()
        group_titles = self.group_parameter_titles()
        indices = [i for i in range(len(all_titles)) 
                   if all_titles[i] in group_titles]

        all_params = self.parameters()
        return tuple(all_params[i] for i in indices)

    def parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that characterize the task.

        :return: The tuple of names of parameters that characterize the task.
        :rtype: Tuple[str]
        """
        return ()

    def parameters(self) -> tuple:
        """
        Return the tuple of parameters that characterize the task.

        :return: The tuple of parameters that characterize the task.
        :rtype: tuple
        """
        return ()
    
    def hardness_parameters(self) -> tuple:
        """
        Return the tuple of parameters determining the hardness of the task. This is to be used to initialize the Hardness object.

        :return: The tuple of parameters determining the hardness of the task. 
        :rtype: tuple
        """        
        return ()
    
    def result_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of output values for the solved task.

        :return: The tuple of names of output values for the solved task.
        :rtype: Tuple[str]
        """
        return ()
    
    def run(self) -> tuple:
        """
        Return the tuple of output values for the solved task.

        :return: The tuple of output values for the solved task.
        :rtype: tuple
        """
        return ()

    def __str__(self) -> str:
        """
        Return the string representation of the task.

        :return: The string representation of the task.
        :rtype: str
        """        
        return str(self.parameters())
    
    def __repr__(self):
        """
        Return the string representation of the task.

        :return: The string representation of the task.
        :rtype: str
        """
        return str(self)