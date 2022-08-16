"""
The implementation of the Branch and Bound (B&B) algorithm.
"""

from __future__ import annotations
from xml.etree.ElementInclude import include
from examples.agent_assignment.instance import *
from src.util import filter_indices, list2str
import time
import sys
from typing import Tuple, List

class Option:
    """
    The options determining the B&B variant.
    """    
    NO_CUTOFFS = "No cutoffs"
    """
    If ``Option.NO_CUTOFFS`` is specified, the B&B cutoffs will not be performed.
    """    
    HEURISTIC = "Heuristic"
    """
    If ``Option.HEURISTIC`` is specified, B&B will use the heuristic.
    """

AgentAssignment = List[int]

def assignment2str(assignment: AgentAssignment) -> str:    
    """
    Translate the agent assignment to string.

    :param assignment: An (partial) agent assignment.
    :type assignment: AgentAssignment
    :return: The string representation of the agent assignment.
    :rtype: str
    """    
    return ";".join([str(a) for a in assignment])

class Algorithm:
    """
    The class representing the B&B algorithm.
    """    
    def __init__(self, options:set, instance: ProblemInstance):
        """
        The constructor.

        :param options: The options controlling the behaviour of the algorithm. 
        :type options: set
        :param instance: The problem instance
        :type instance: ProblemInstance
        """        
        self.instance = instance
        self.tasks = instance.tasks
        self.options = options
        self.no_cutoffs_flag = (Option.NO_CUTOFFS in options)
        self.heuristic_flag = (Option.HEURISTIC in options)
        
        self.opt_cost = sys.maxsize
        self.opt_assignment = None
        self.expanded = 0
        self.time = 0

    def result_titles(self) -> Tuple[str]:
        """
        Names of quantities in the result tuple. 

        :return: Names of quantities in the result tuple.
        :rtype: Tuple[str]
        """        
        return ("Exp", "Time", "Cost", "Assgn")

    def search(self) -> tuple:
        """
        Performs B&B search to solve the problem instance.

        :return: The results of the search.
        :rtype: tuple
        """        
        before = time.time()
        self._search([], 0)
        assignment_str = list2str(self.opt_assignment)
        self.time += (time.time() - before)
        return (self.expanded, round(self.time, 5), self.opt_cost, assignment_str)
        
    def _search(self, assignment: AgentAssignment, g: float):
        """
        Performs B&B search to solve the problem instance. This method is called by :func:`search` and should not be called directly.

        :param assignment: The current parial assignment.
        :type assignment: AgentAssignment
        :param g: The time used for completion of the tasks in :paramref:`assignment`.
        :type g: float
        """        
        f = g + self._heuristic(assignment)
        if (not self.no_cutoffs_flag) and f > self.opt_cost: return
        if self._full_assignment(assignment):
            if f < self.opt_cost:
                self.opt_cost = f
                self.opt_assignment = assignment
            return
        
        self.expanded += 1
        
        t = self.tasks[len(assignment)]
        for i, cost in enumerate(t.agent_costs):
            if i in assignment: continue
            self._search(assignment + [i], g + cost)
    
    def _full_assignment(self, assignment: AgentAssignment) -> bool:
        """
        Check whether the assignment is already complete, i.e. all tasks got an agent assigned.

        :param assignment: The current parial assignment.
        :type assignment: AgentAssignment
        :return: ``True`` if the assignment is complete and ``False`` otherwise.
        :rtype: bool
        """        
        return len(assignment) == len(self.tasks)

    def _heuristic(self, assignment: AgentAssignment) -> float:
        """
        Computes the heuristic. The heuristic is computed by chosing the best of the remaining agents for each of the remaining tasks and summing the times taken by the chosen agents to perform the remaining tasks.

        :param assignment: The current partial assignhment of agents to tasks.
        :type assignment: AgentAssignment
        :return: The heuristic estimate.
        :rtype: float
        """        
        if (not self.heuristic_flag) or (len(assignment) == len(self.tasks)):
            return 0
        h = 0
        for t in self.tasks[len(assignment):]:
            h += min(filter_indices(t.agent_costs, lambda i: i not in assignment))
        return h