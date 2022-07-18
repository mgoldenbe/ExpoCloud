from examples.agent_assignment.instance import *
from src.util import filter_indices, list2str
import time
import sys

class Option:
    NO_CUTOFFS = "No cutoffs"
    HEURISTIC = "Heuristic"

def assignment2str(assignment):
    return ";".join([str(a) for a in assignment])

class Algorithm:
    def __init__(self, options, instance, verbose_level = 0):
        self.instance = instance
        self.tasks = instance.tasks
        self.options = options
        self.no_cutoffs_flag = (Option.NO_CUTOFFS in options)
        self.heuristic_flag = (Option.HEURISTIC in options)
        self.verbose_level = verbose_level
        
        self.opt_cost = sys.maxsize
        self.opt_assignment = None
        self.expanded = 0
        self.time = 0

    def result_titles(self):
        return ("Exp", "Time", "Cost", "Assgn")

    def search(self):
        before = time.time()
        self._search([], 0)
        assignment_str = list2str(self.opt_assignment)
        self.time += (time.time() - before)
        return (self.expanded, round(self.time, 5), self.opt_cost, assignment_str)
        
    def _search(self, assignment, g):
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
    
    def _full_assignment(self, assignment):
        return len(assignment) == len(self.tasks)

    def _heuristic(self, assignment):
        if (not self.heuristic_flag) or (len(assignment) == len(self.tasks)):
            return 0
        h = 0
        for t in self.tasks[len(assignment):]:
            h += min(filter_indices(t.agent_costs, lambda i: i not in assignment))
        return h