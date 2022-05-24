from examples.samd.instance import *

class Option:
    NO_CUTOFFS = "No cutoffs"
    HEURISTIC = "Heuristic"
    EPSILON = "Epsilon"
    EPSILON_PERFECT = "Epsilon Perfect"
    
class Algorithm:
    def __init__(self, options, instance, m = -1, verbose_level = 0):
        self._instance = instance
        self._tasks, self._tasks_super = instance.tasks, instance.tasks_super
        self._options = options
        self._no_cutoffs_flag = (Option.NO_CUTOFFS in options)
        self._heuristic_flag = (Option.HEURISTIC in options)
        self._epsilon_flag = (Option.EPSILON in options)
        self._epsilon_perfect_flag = (Option.EPSILON_PERFECT in options)
        if (not self._epsilon_flag and not self._epsilon_perfect_flag): assert(m == -1)
        self._deadline = instance.deadline
        self._m = m
        self._verbose_level = verbose_level
        self._opt_prob = 0
        self._opt_prob_epsilon = -1
        self._opt_assignment = []
        self._expanded = 0
        self._time = (instance.time_super if self._heuristic_flag else 0)

    def result_titles(self):
        return ("M_eps", "P", "Time", "Assignment")

    def search(self):
        try:
            before = time.time()
            self._search([], Distribution({0:1}), 1.0) # Distribution({0:1} + d == d
            if self._epsilon_flag or self._epsilon_perfect_flag:
                self._opt_prob_epsilon = self._opt_prob
                self._opt_prob = self._assignment_to_prob(self._opt_assignment)
            assignment_str = ','.join([s.name() for s in self._opt_assignment])
            self._time += (time.time() - before)
            return \
                 (round(self._opt_prob_epsilon, 6), round(self._opt_prob, 3), self._expanded, round(self._time, 5), assignment_str)
        except:
            return self._instance.parameters() + (self._options, self._m, -1, -1, -1, -1, -1)
        
    def _search(self, assignment, distr, parent_utility):
        utility = self._utility(assignment, distr)
        utility_with_min = self._utility_with_min(utility, parent_utility)
        if utility_with_min < self._opt_prob and not self._no_cutoffs_flag:
            return
        if self._full_assignment(assignment):
            if utility > self._opt_prob:
                self._opt_prob = utility
                self._opt_assignment = assignment
            return
        
        self._expanded += 1; self._verbose(assignment)#; print(self._expanded)
        
        t = self._tasks[len(assignment)]
        for supplier in t.suppliers():
            self._search(assignment + [supplier], self._child_distr(distr, supplier), utility_with_min)
    
    def _full_assignment(self, assignment):
        return len(assignment) == len(self._tasks)
    
    def _heuristic_distr(self, assignment):
        if (not self._heuristic_flag) or (len(assignment) == len(self._tasks)):
            return Distribution({0:1})
        return self._tasks_super[len(assignment)].suppliers()[0].distr()
    
    def _utility(self, assignment, distr):
        #full_distr = distr.add(self._heuristic_distr(assignment), self._m if not self._epsilon_perfect_flag else -1)
        full_distr = distr.add(self._heuristic_distr(assignment))
        if self._epsilon_perfect_flag and self._full_assignment(assignment):
            full_distr = full_distr.approx(self._m)
        return full_distr.prob_before_deadline(self._deadline)
    
    def _utility_with_min(self, utility, parent_utility):
        if not self._epsilon_flag: return utility
        return min(utility + (len(self._instance.tasks)-1)/self._m, parent_utility)
    
    def _child_distr(self, distr, supplier):
        return distr.add(supplier.distr(), self._m if not self._epsilon_perfect_flag else -1)
    
    def _verbose(self, assignment):
        if self._verbose_level > 1:
            if (len(assignment) > 0):
                name_last = assignment[-1].name()
                print(4 * ' ' * int(name_last[0]) + str(name_last))
    
    def _assignment_to_prob(self, assignment):
        if not assignment: return 0
        distr = Distribution({0:1})
        for supplier in assignment:
            distr += supplier.distr()
        return distr.prob_before_deadline(self._deadline)
    
    def assignment_debug(self, my_assignment):
        distr = Distribution({0:1})
        parent_prob = 1.0
        my_prob = -1
        print("Assigment: ", assignment2str(my_assignment))
        for i in range(len(my_assignment) + 1):
            assignment = my_assignment[:i]
            utility = self._utility(assignment, distr)
            utility_with_min = self._utility_with_min(utility, parent_prob)
            print(f"\nAssignment {assignment2str(assignment)}    Distribution: {distr}")
            print(f"\nUtility: {utility:.3f}     Utility with min: {utility_with_min: .3f}")
            
            if i == len(my_assignment): break
            supplier = my_assignment[i]
            distr = self._child_distr(distr, supplier)
            parent_prob = utility_with_min